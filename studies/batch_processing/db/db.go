package db

import (
	"context"
	"fmt"
	"studies/env"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool struct {
	pool *pgxpool.Pool
}

func NewPool() *Pool {
	conf, parseErr := pgxpool.ParseConfig("host=" + env.PostgresHost + " user=" + env.PostgresUser + " password=" + env.PostgresPassword + " dbname=" + env.PostgresDb + " port=5432")
	if parseErr != nil {
		panic(parseErr)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), conf)
	if err != nil {
		panic(err)
	}
	return &Pool{pool: pool}
}

func (p *Pool) Close() {
	p.pool.Close()
}

type Client struct {
	conn *pgxpool.Conn
}

func (p *Pool) GetClient() *Client {
	conn, err := p.pool.Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	return &Client{conn: conn}
}

func (c *Client) Close() {
	c.conn.Release()
}

func (c *Client) Query(query string, args ...interface{}) pgx.Rows {
	rows, err := c.conn.Query(context.Background(), query, args...)
	if err != nil {
		panic(err)
	}
	return rows
}

func GetCellsAllDatastreamsQuery(cellTimestamps [4][2]int32) string {
	if len(cellTimestamps) == 0 {
		panic("cellTimestamps must not be empty")
	}

	timestampsString := "("
	for _, timestamp := range cellTimestamps {
		timestampsString += "(phenomenon_time >=" + fmt.Sprint(timestamp[0]) + " AND phenomenon_time <=" + fmt.Sprint(timestamp[1]) + ") OR "
	}
	timestampsString = timestampsString[:len(timestampsString)-4] + ")"

	query := `
	SELECT
		phenomenon_time,result,datastream_id
	FROM
		observation_dbs
	WHERE
		` + timestampsString + `
	ORDER BY
		phenomenon_time ASC`

	return query
}
