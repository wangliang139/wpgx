package wpgx

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"
)

// WTx is a wrapped pgx.Tx. The main reason is to overwrite the PostExec method that
// run all of them until the transaction is successfully committed.
type WTx struct {
	tx            pgx.Tx
	stats         *metricSet
	postExecFuncs []PostExecFunc
	mutex         sync.Mutex
}

var _ Exec = (*WTx)(nil)

func (t *WTx) runPostExecFuncs() {
	var wg sync.WaitGroup
	for _, v := range t.postExecFuncs {
		wg.Add(1)
		go func(fn PostExecFunc) {
			defer wg.Done()
			err := fn()
			if err != nil {
				log.Err(err).Msgf("Failed to run post exec function")
			}
		}(v)
	}
	wg.Wait()
}

func (t *WTx) PostExec(f PostExecFunc) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.postExecFuncs = append(t.postExecFuncs, f)
	return nil
}

func (t *WTx) WQuery(ctx context.Context, name string, unprepared string, args ...interface{}) (pgx.Rows, error) {
	defer t.stats.Observe(name, time.Now())()
	r, err := t.tx.Query(ctx, unprepared, args...)
	if err != nil {
		return r, err
	}
	return r, nil
}

func (t *WTx) WQueryRow(ctx context.Context, name string, unprepared string, args ...interface{}) pgx.Row {
	defer t.stats.Observe(name, time.Now())()
	return t.tx.QueryRow(ctx, unprepared, args...)
}

func (t *WTx) WExec(ctx context.Context, name string, unprepared string, args ...interface{}) (pgconn.CommandTag, error) {
	defer t.stats.Observe(name, time.Now())()
	return t.tx.Exec(ctx, unprepared, args...)
}

func (t *WTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

func (t *WTx) Commit(ctx context.Context) error {
	err := t.tx.Commit(ctx)
	if err != nil {
		return err
	}
	t.runPostExecFuncs()
	return nil
}
