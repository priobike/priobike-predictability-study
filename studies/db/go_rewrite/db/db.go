package db

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type Client struct {
	conn *pgx.Conn
}

func NewClient() *Client {
	conf, parseErr := pgx.ParseConfig("user=postgres password=Et7RvZ4TjEBHRF host=priobike-sentry.inf.tu-dresden.de port=443 dbname=observations")
	if parseErr != nil {
		panic(parseErr)
	}
	conn, err := pgx.ConnectConfig(context.Background(), conf)
	if err != nil {
		panic(err)
	}
	return &Client{conn: conn}
}

func (c *Client) Close() {
	c.conn.Close(context.Background())
}

func (c *Client) Query(query string, args ...interface{}) (pgx.Rows) {
	rows, err := c.conn.Query(context.Background(), query, args...)
	if err != nil {
		panic(err)
	}
	return rows
}
