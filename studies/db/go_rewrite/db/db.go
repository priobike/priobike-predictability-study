package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool struct {
	pool *pgxpool.Pool
}

func NewPool() *Pool {
	conf, parseErr := pgxpool.ParseConfig("user=postgres password=Et7RvZ4TjEBHRF host=localhost port=5432 dbname=postgres")
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

func GetCellsQuery(datastreamIds []int32, cellTimestamps [][2]int32) string {
	/* if len(cellTimestamps) == 0 {
		panic("cellTimestamps must not be empty")
	}

	idsString := "("
	for _, datastreamId := range datastreamIds {
		idsString += fmt.Sprint(datastreamId) + ","
	}
	idsString = idsString[:len(idsString)-1] + ")"

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
		datastream_id IN ` + idsString + `
		AND ` + timestampsString + `
	ORDER BY
		phenomenon_time ASC`

	return query */
	return ""
}

func GetCellQuery(datastreamIds []int32, cellTimestamps [2]int32) string {
	/* if len(cellTimestamps) == 0 {
			panic("cellTimestamps must not be empty")
		}

		idsString := "("
		for _, datastreamId := range datastreamIds {
			idsString += fmt.Sprint(datastreamId) + ","
		}
		idsString = idsString[:len(idsString)-1] + ")"

		timestampsString := "(phenomenon_time >=" + fmt.Sprint(cellTimestamps[0]) + " AND phenomenon_time <=" + fmt.Sprint(cellTimestamps[1]) + ")"

		query := `
	    SELECT
	        phenomenon_time,result,datastream_id
	    FROM
	        observation_dbs
	    WHERE
	        datastream_id IN ` + idsString + `
	        AND ` + timestampsString + `
	    ORDER BY
	        phenomenon_time ASC`

		return query */
	return ""
}

func GetThingQuery(datastreamIds []int32) string {
	/* if len(datastreamIds) != 2 {
			panic("cellTimestamps must not be empty")
		}

		idsString := "("
		for _, datastreamId := range datastreamIds {
			idsString += fmt.Sprint(datastreamId) + ","
		}
		idsString = idsString[:len(idsString)-1] + ")"

		query := `
	    SELECT
	        phenomenon_time,result,datastream_id
	    FROM
	        observation_dbs
	    WHERE
	        datastream_id IN ` + idsString + `
	    ORDER BY
	        phenomenon_time ASC`

		return query */
	return ""
}

/* func GetDayQuery(datastreamIds []int32, dayTimestamps [2][4][2]int32) string {
	if len(dayTimestamps) == 0 {
		panic("dayTimestamps must not be empty")
	}

	idsString := "("
	for _, datastreamId := range datastreamIds {
		idsString += fmt.Sprint(datastreamId) + ","
	}
	idsString = idsString[:len(idsString)-1] + ")"

	timestampsString := "("
	for _, day := range dayTimestamps {
		for _, timestamp := range day {
			timestampsString += "(phenomenon_time >=" + fmt.Sprint(timestamp[0]) + " AND phenomenon_time <=" + fmt.Sprint(timestamp[1]) + ") OR "
		}
	}
	timestampsString = timestampsString[:len(timestampsString)-4] + ")"

	query := `
	SELECT
		phenomenon_time,result,datastream_id
	FROM
		observation_dbs
	WHERE
		datastream_id IN ` + idsString + `
		AND ` + timestampsString + `
	ORDER BY
		phenomenon_time ASC`

	return query
} */
