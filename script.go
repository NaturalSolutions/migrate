package main

import (
	"database/sql"
	"log"
	"strings"
)

type SqlScript struct {
	Db        string
	Name      string
	Number    int
	Content   string
	Installed bool
}

type QueryResult struct {
	query  string
	err    error
	result sql.Result
}

func (s SqlScript) Execute(conn *sql.DB) ([]QueryResult, error) {
	txn, err := conn.Begin()
	if err != nil {
		return nil, err
	}

	results := []QueryResult{}
	for _, query := range strings.Split(s.Content, "GO") {
		var r QueryResult
		r.query = query
		r.result, r.err = txn.Exec(r.query)
		results = append(results, r)

		if *verbose {
			log.Println("SQL>\n", r.query)
		}

		if r.err != nil {
			return results, r.err
		}

		if *verbose {
			i, err := r.result.RowsAffected()
			if err == nil {
				log.Printf("rows affected: %d", i)
			}

			i, err = r.result.LastInsertId()
			if err == nil {
				log.Printf("last insert id: %d", i)
			}
		}
	}

	err = txn.Commit()
	return results, err
}
