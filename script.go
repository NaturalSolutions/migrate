package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
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

// parseStatements tries to extract each subquery in s.Content
// by splitting them between "GO" instruction.
func (s SqlScript) ParseStatements() []string {
	var stmts []string
	var stmt string

	lines := strings.Split(s.Content, "\n")
	for _, line := range lines {
		if strings.ToUpper(strings.TrimSpace(line)) == "GO" {
			stmts = append(stmts, stmt)
			stmt = ""
			continue
		}
		stmt += fmt.Sprintln(line)
	}
	return stmts
}

func (s SqlScript) Execute(conn *sql.DB) ([]QueryResult, error) {
	txn, err := conn.Begin()
	if err != nil {
		return nil, err
	}

	results := []QueryResult{}
	for i, query := range s.ParseStatements() {
		var r QueryResult
		r.query = query
		r.result, r.err = txn.Exec(r.query)
		results = append(results, r)

		if *verbose {
			log.Printf("%d> %s", i, r.query)
		}

		if r.err != nil {
			_ = txn.Rollback()
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
			fmt.Fprintln(os.Stderr)
		}
	}

	err = txn.Commit()
	if err != nil {
		_ = txn.Rollback()
	}
	return results, err
}
