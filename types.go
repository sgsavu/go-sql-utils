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

type DBConnection struct {
	Type DatabaseType `reqHeader:"X-Db-Type"`
	Host string       `reqHeader:"X-Db-Host"`
	Port string       `reqHeader:"X-Db-Port"`
	Name string       `reqHeader:"X-Db-Name"`
	User string       `reqHeader:"X-Db-User"`
	Pass string       `reqHeader:"X-Db-Pass"`
}

type TableRecord map[string]interface{}
