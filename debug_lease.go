package main

import (
	"database/sql"
	"fmt"
	_ "modernc.org/sqlite"
)

func main() {
	db, _ := sql.Open("sqlite", "debug.db")
	defer db.Close()
	db.Exec(`CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY, status TEXT, lease_expires_at DATETIME)`)
	db.Exec(`INSERT INTO runs (id,status) VALUES (1,'pending')`)
	res, err := db.Exec(`UPDATE runs SET status='running', lease_expires_at=datetime('now','+30 seconds') WHERE id=1 AND status IN ('pending','running','failed') AND (lease_expires_at IS NULL OR lease_expires_at < CURRENT_TIMESTAMP)`)
	fmt.Println(err)
	n, _ := res.RowsAffected()
	fmt.Println("rows:", n)
	row := db.QueryRow(`SELECT status, lease_expires_at FROM runs WHERE id=1`)
	var st, le string
	row.Scan(&st, &le)
	fmt.Println(st, le)
}
