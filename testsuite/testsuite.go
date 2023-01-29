package testsuite

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"

	"github.com/stumble/wpgx"
)

type Loader interface {
	Load(data []byte) error
}

type Dumper interface {
	Dump() ([]byte, error)
}

const (
	TestDataDirPath = "testdata"
)

var update = flag.Bool("update", false, "update .golden files")

type WPgxTestSuite struct {
	suite.Suite
	Testdb string
	Tables []string
	Config *wpgx.Config
	Pool   *wpgx.Pool
}

// NewWPgxTestSuiteFromEnv @p db is the name of test db and tables are table creation
// SQL statements. DB will be created, so does tables, on SetupTest.
// If you pass different @p db for suites in different packages, you can test them in parallel.
func NewWPgxTestSuiteFromEnv(db string, tables []string) *WPgxTestSuite {
	config := wpgx.ConfigFromEnv()
	config.DBName = db
	return NewWPgxTestSuiteFromConfig(config, db, tables)
}

// NewWPgxTestSuiteFromConfig connect to PostgreSQL Server according to @p config,
// @p db is the name of test db and tables are table creation
// SQL statements. DB will be created, so does tables, on SetupTest.
// If you pass different @p db for suites in different packages, you can test them in parallel.
func NewWPgxTestSuiteFromConfig(config *wpgx.Config, db string, tables []string) *WPgxTestSuite {
	return &WPgxTestSuite{
		Testdb: db,
		Tables: tables,
		Config: config,
	}
}

// returns a raw *pgx.Pool
func (suite *WPgxTestSuite) GetRawPool() *pgxpool.Pool {
	return suite.Pool.RawPool()
}

// setup the database to a clean state: tables have been created according to the
// schema, empty.
func (suite *WPgxTestSuite) SetupTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// create DB
	conn, err := pgx.Connect(context.Background(), fmt.Sprintf(
		"postgres://%s:%s@%s:%d",
		suite.Config.Username, suite.Config.Password, suite.Config.Host, suite.Config.Port))
	suite.Require().NoError(err)
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s;", suite.Testdb))
	suite.Require().NoError(err)
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s;", suite.Testdb))
	suite.Require().NoError(err)

	// create manager
	pool, err := wpgx.NewPool(context.Background(), suite.Config)
	suite.Require().NoError(err)
	suite.Pool = pool
	suite.Require().NoError(suite.Pool.Ping(context.Background()))

	// create tables
	for _, v := range suite.Tables {
		exec := suite.Pool.WConn()
		_, err := exec.WExec(ctx, "make_table", v)
		suite.Require().NoError(err)
	}
}

func (suite *WPgxTestSuite) TearDownTest() {
	if suite.Pool != nil {
		suite.Pool.Close()
	}
}

// load bytes from file
func (suite *WPgxTestSuite) loadFile(file string) []byte {
	suite.Require().FileExists(file)
	f, err := os.Open(file)
	suite.Require().NoError(err)
	defer f.Close()
	data, err := io.ReadAll(f)
	suite.Require().NoError(err)
	return data
}

// LoadState load state from the file to DB.
// For example LoadState(ctx, "sample1.input.json") will load (insert) from
// "testdata/sample1.input.json" to table
func (suite *WPgxTestSuite) LoadState(filename string, loader Loader) {
	input := testDirFilePath(filename)
	data := suite.loadFile(input)
	suite.Require().NoError(loader.Load(data))
}

// Dumpstate dump state to the file.
// For example DumpState(ctx, "sample1.golden.json") will dump (insert) bytes from
// dumper.dump() to "testdata/sample1.golden.json".
func (suite *WPgxTestSuite) DumpState(filename string, dumper Dumper) {
	outputFile := testDirFilePath(filename)
	dir, _ := filepath.Split(outputFile)
	suite.Require().NoError(ensureDir(dir))
	f, err := os.Create(outputFile)
	suite.Require().NoError(err)
	defer f.Close()
	bytes, err := dumper.Dump()
	suite.Require().NoError(err)
	_, err = f.Write(bytes)
	suite.Require().NoError(err)
	suite.Require().NoError(f.Sync())
}

func (suite *WPgxTestSuite) Golden(dbName string, dumper Dumper) {
	goldenFile := fmt.Sprintf("%s.%s.golden", suite.T().Name(), dbName)
	if *update {
		suite.DumpState(goldenFile, dumper)
		return
	}
	golden := suite.loadFile(testDirFilePath(goldenFile))
	state, err := dumper.Dump()
	suite.Require().NoError(err)
	suite.Equal(golden, state)
}

func testDirFilePath(filename string) string {
	return filepath.Join(TestDataDirPath, filename)
}

func ensureDir(dirName string) error {
	err := os.MkdirAll(dirName, 0700)
	if err == nil || os.IsExist(err) {
		return nil
	} else {
		return err
	}
}
