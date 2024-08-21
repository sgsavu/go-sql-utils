package sqlutils

import (
	"database/sql"
	"fmt"

	_ "github.com/cockroachdb/cockroach-go/v2/crdb" // CockroachDB
	_ "github.com/denisenkom/go-mssqldb"            // SQL Server
	_ "github.com/go-sql-driver/mysql"              // MySQL
	_ "github.com/godror/godror"                    // Oracle
	_ "github.com/lib/pq"                           // PostgreSQL
	_ "github.com/mattn/go-sqlite3"                 // SQLite
)

func ConnectDB(connInfo *SQLConnectionInfo) (*sql.DB, error) {
	connStr, err := getConnectionString(connInfo)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(string(connInfo.Type), connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %v", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	return db, nil
}
