package testsuite_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/stumble/wpgx"
	sqlsuite "github.com/stumble/wpgx/testsuite"
)

type metaTestSuite struct {
	*sqlsuite.PgxTestSuite
}

type Doc struct {
	Id          int             `json:"id"`
	Rev         float64         `json:"rev"`
	Content     string          `json:"content"`
	CreatedAt   time.Time       `json:"created_at"`
	Description json.RawMessage `json:"description"`
}

type loaderDumper struct {
	exec wpgx.WGConn
}

func (m *loaderDumper) Dump() ([]byte, error) {
	rows, err := m.exec.WQuery(
		context.Background(),
		"dump",
		"SELECT id,rev,content,created_at,description FROM docs")
	if err != nil {
		return nil, err
	}
	results := make([]Doc, 0)
	for rows.Next() {
		row := Doc{}
		err := rows.Scan(&row.Id, &row.Rev, &row.Content, &row.CreatedAt, &row.Description)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	bytes, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (m *loaderDumper) Load(data []byte) error {
	docs := make([]Doc, 0)
	err := json.Unmarshal(data, &docs)
	if err != nil {
		return err
	}
	for _, doc := range docs {
		_, err := m.exec.WExec(
			context.Background(),
			"load",
			"INSERT INTO docs (id,rev,content,created_at,description) VALUES ($1,$2,$3,$4,$5)",
			doc.Id, doc.Rev, doc.Content, doc.CreatedAt, doc.Description)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewMetaTestSuite() *metaTestSuite {
	return &metaTestSuite{
		PgxTestSuite: sqlsuite.NewPgxTestSuiteFromEnv("metatestdb", []string{
			`CREATE TABLE IF NOT EXISTS docs (
               id          INT NOT NULL,
               rev         DOUBLE PRECISION NOT NULL,
               content     VARCHAR(200) NOT NULL,
               created_at  TIMESTAMP NOT NULL,
               description JSON NOT NULL,
               PRIMARY KEY(id)
             );`,
		}),
	}
}

func TestMetaTestSuite(t *testing.T) {
	suite.Run(t, NewMetaTestSuite())
}

func (suite *metaTestSuite) SetupTest() {
	suite.PgxTestSuite.SetupTest()
}

func (suite *metaTestSuite) TestInsertQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exec := suite.Pool.WConn()
	rst, err := exec.WExec(ctx,
		"insert",
		"INSERT INTO docs (id, rev, content, created_at, description) VALUES ($1,$2,$3,$4,$5)",
		33, 666.7777, "hello world", time.Now(), json.RawMessage("{}"))
	suite.Nil(err)
	n := rst.RowsAffected()
	suite.Equal(int64(1), n)

	exec = suite.Pool.WConn()
	rows, err := exec.WQuery(ctx, "select_content",
		"SELECT content FROM docs WHERE id = $1", 33)
	suite.Nil(err)
	defer rows.Close()

	content := ""
	suite.True(rows.Next())
	err = rows.Scan(&content)
	suite.Nil(err)
	suite.Equal("hello world", content)
}

func (suite *metaTestSuite) TestInsertUseGolden() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exec := suite.Pool.WConn()
	rst, err := exec.WExec(ctx,
		"insert_data",
		"INSERT INTO docs (id, rev, content, created_at, description) VALUES ($1,$2,$3,$4,$5)",
		33, 666.7777, "hello world", time.Unix(1000, 0), json.RawMessage("{}"))
	suite.Nil(err)
	n := rst.RowsAffected()
	suite.Equal(int64(1), n)
	dumper := &loaderDumper{exec: exec}
	suite.PgxTestSuite.Golden("docs", dumper)
}

func (suite *metaTestSuite) TestQueryUseLoader() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exec := suite.Pool.WConn()

	// load state to db from input
	loader := &loaderDumper{exec: exec}
	suite.PgxTestSuite.LoadState("TestMetaTestSuite/TestQueryUseLoader.docs.json", loader)

	rows, err := exec.WQuery(ctx,
		"select_all",
		"SELECT content, rev, created_at, description FROM docs WHERE id = $1", 33)
	suite.Nil(err)
	defer rows.Close()

	var content string
	var rev float64
	var createdAt time.Time
	var desc json.RawMessage
	suite.True(rows.Next())
	err = rows.Scan(&content, &rev, &createdAt, &desc)
	suite.Nil(err)
	suite.Equal("content read from file", content)
	suite.Equal(float64(66.66), rev)
	suite.Equal(int64(1000), createdAt.Unix())
	suite.Equal(json.RawMessage(`{"github_url":"github.com/stumble/wpgx"}`), desc)
}
