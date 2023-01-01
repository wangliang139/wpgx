package wpgx

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type WConn struct {
	p *Pool
}

var _ WGConn = (*WConn)(nil)

func (c *WConn) PostExec(fn PostExecFunc) error {
	return fn()
}

func (c *WConn) WQuery(ctx context.Context, name string, unprepared string, args ...interface{}) (pgx.Rows, error) {
	defer c.p.stats.Observe(name, time.Now())()
	r, err := c.p.pool.Query(ctx, unprepared, args...)
	if err != nil {
		return r, err
	}
	return r, nil
}

func (c *WConn) WQueryRow(ctx context.Context, name string, unprepared string, args ...interface{}) pgx.Row {
	defer c.p.stats.Observe(name, time.Now())()
	return c.p.pool.QueryRow(ctx, unprepared, args...)
}

func (c *WConn) WExec(ctx context.Context, name string, unprepared string, args ...interface{}) (pgconn.CommandTag, error) {
	defer c.p.stats.Observe(name, time.Now())()
	return c.p.pool.Exec(ctx, unprepared, args...)
}

// pgx did the right thing: prepare should not be visible to a connection pool.
// func (c *WConn) Prepare(ctx context.Context, query string) (pgconn.StatementDescription, error) {
// 	return c.conn.Prepare(ctx, query)
// }
