package sqlutils

type DatabaseType string

const (
	PostgreSQL  DatabaseType = "postgres"
	MySQL       DatabaseType = "mysql"
	SQLite      DatabaseType = "sqlite3"
	SQLServer   DatabaseType = "sqlserver"
	Oracle      DatabaseType = "oracle"
	MariaDB     DatabaseType = "mariadb"
	CockroachDB DatabaseType = "cockroachdb"
)

type SQLConnectionInfo struct {
	Type   DatabaseType `json:"type"`
	Host   string       `json:"host"`
	Port   string       `json:"port"`
	DBName string       `json:"dbName"`
	User   string       `json:"user"`
	Passwd string       `json:"password"`
}

type TableRecord map[string]interface{}
