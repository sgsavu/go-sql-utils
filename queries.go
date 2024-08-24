package sqlutils

import (
	"fmt"
	"strings"
)

var IsTableExistentQueryTemplate = map[DatabaseType]string{
	MySQL:       "SELECT 1 FROM `%s`;",
	MariaDB:     "SELECT 1 FROM `%s`;",
	SQLServer:   "SELECT 1 FROM %s;",
	PostgreSQL:  "SELECT 1 FROM \"%s\";",
	SQLite:      "SELECT 1 FROM `%s`;",
	Oracle:      "SELECT 1 FROM %s;",
	CockroachDB: "SELECT 1 FROM \"%s\";",
}

func getQueryForIsTableExistent(dbName string, databaseType DatabaseType) (string, error) {
	template, ok := IsTableExistentQueryTemplate[databaseType]
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}
	return fmt.Sprintf(template, dbName), nil
}

var tablesQueryTemplates = map[DatabaseType]string{
	MySQL:       "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s';",
	MariaDB:     "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s';",
	SQLServer:   "SELECT table_name FROM information_schema.tables WHERE table_catalog = '%s' AND table_schema = 'dbo';",
	PostgreSQL:  "SELECT table_name FROM information_schema.tables WHERE table_catalog = '%s' AND table_schema = 'public';",
	SQLite:      "SELECT name FROM sqlite_master WHERE type='table';",
	Oracle:      "SELECT table_name FROM all_tables WHERE owner = '%s';",
	CockroachDB: "SELECT table_name FROM information_schema.tables WHERE table_catalog = '%s' AND table_schema = 'public';",
}

func getQueryForTables(dbName string, databaseType DatabaseType) (string, error) {
	template, ok := tablesQueryTemplates[databaseType]
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}
	return fmt.Sprintf(template, dbName), nil
}

var allRecordsQueryTemplates = map[DatabaseType]string{
	MySQL:       "SELECT * FROM `%s`;",
	MariaDB:     "SELECT * FROM `%s`;",
	SQLServer:   "SELECT * FROM dbo.[%s];",
	PostgreSQL:  "SELECT * FROM \"%s\";",
	SQLite:      "SELECT * FROM \"%s\";",
	Oracle:      "SELECT * FROM \"%s\";",
	CockroachDB: "SELECT * FROM \"public\".\"%s\";",
}

func getQueryForAllRecords(tableName string, databaseType DatabaseType) (string, error) {
	template, ok := allRecordsQueryTemplates[databaseType]
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}
	return fmt.Sprintf(template, tableName), nil
}

var columnQueryTemplates = map[DatabaseType]string{
	MySQL:       "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION;",
	MariaDB:     "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION;",
	SQLServer:   "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = 'dbo' ORDER BY ORDINAL_POSITION;",
	PostgreSQL:  "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = $1 AND TABLE_SCHEMA = 'public' ORDER BY ORDINAL_POSITION;",
	SQLite:      "PRAGMA table_info(?);",
	Oracle:      "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = UPPER(?) AND OWNER = USER ORDER BY COLUMN_ID;",
	CockroachDB: "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = $1 AND TABLE_SCHEMA = 'public' ORDER BY ORDINAL_POSITION;",
}

func getQueryForColumns(databaseType DatabaseType) (string, error) {
	template, ok := columnQueryTemplates[databaseType]
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}
	return template, nil
}

var primaryKeyQueryTemplates = map[DatabaseType]string{
	MySQL:       "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION;",
	MariaDB:     "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION;",
	SQLServer:   "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_CATALOG = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION;",
	PostgreSQL:  "SELECT a.attname AS column_name FROM pg_constraint AS c JOIN pg_attribute AS a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid WHERE c.contype = 'p' AND c.conrelid = $1::regclass;",
	SQLite:      "PRAGMA table_info(?);",
	Oracle:      "SELECT COLUMN_NAME FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME = (SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND TABLE_NAME = UPPER(?) AND OWNER = USER) AND TABLE_NAME = UPPER(?) ORDER BY POSITION;",
	CockroachDB: "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_CATALOG = $1 AND TABLE_NAME = $2 AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION;",
}

func getQueryForPrimaryKeys(databaseType DatabaseType) (string, error) {
	template, ok := primaryKeyQueryTemplates[databaseType]
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}
	return template, nil
}

var dropTableQueryTemplates = map[DatabaseType]string{
	MySQL:       "DROP TABLE `%s`;",
	MariaDB:     "DROP TABLE `%s`;",
	SQLServer:   "DROP TABLE [%s];",
	PostgreSQL:  "DROP TABLE \"%s\";",
	SQLite:      "DROP TABLE `%s`;",
	Oracle:      "BEGIN EXECUTE IMMEDIATE 'DROP TABLE %s'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;",
	CockroachDB: "DROP TABLE \"%s\";",
}

func getQueryForDeleteTable(databaseType DatabaseType) (string, error) {
	template, ok := dropTableQueryTemplates[databaseType]
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}
	return template, nil
}

var renameTableQueryTemplates = map[DatabaseType]string{
	MySQL:       "RENAME TABLE `%s` TO `%s`;",
	MariaDB:     "RENAME TABLE `%s` TO `%s`;",
	SQLServer:   "EXEC sp_rename '%s', '%s';",
	PostgreSQL:  "ALTER TABLE \"%s\" RENAME TO \"%s\";",
	SQLite:      "ALTER TABLE `%s` RENAME TO `%s`;",
	Oracle:      "ALTER TABLE %s RENAME TO %s;",
	CockroachDB: "ALTER TABLE \"%s\" RENAME TO \"%s\";",
}

func getQueryForRenameTable(databaseType DatabaseType) (string, error) {
	template, ok := renameTableQueryTemplates[databaseType]
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}
	return template, nil
}

var duplicateCreateTableQueryTemplates = map[DatabaseType]string{
	MySQL:       "CREATE TABLE `%s` LIKE `%s`;",
	MariaDB:     "CREATE TABLE `%s` LIKE `%s`;",
	SQLServer:   "SELECT * INTO %s FROM %s WHERE 1 = 0;",
	PostgreSQL:  "CREATE TABLE \"%s\" (LIKE \"%s\" INCLUDING ALL);",
	SQLite:      "CREATE TABLE `%s` AS SELECT * FROM `%s` WHERE 1 = 0;",
	Oracle:      "CREATE TABLE %s AS SELECT * FROM %s WHERE 1 = 0;",
	CockroachDB: "CREATE TABLE \"%s\" (LIKE \"%s\" INCLUDING ALL);",
}

func getQueryForDuplicateTableCreate(databaseType DatabaseType) (string, error) {
	template, ok := duplicateCreateTableQueryTemplates[databaseType]
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}
	return template, nil
}

var duplicateInsertDataQueryTemplates = map[DatabaseType]string{
	MySQL:       "INSERT INTO `%s` SELECT * FROM `%s`;",
	MariaDB:     "INSERT INTO `%s` SELECT * FROM `%s`;",
	SQLServer:   "INSERT INTO %s SELECT * FROM %s;",
	PostgreSQL:  "INSERT INTO \"%s\" SELECT * FROM \"%s\";",
	SQLite:      "INSERT INTO `%s` SELECT * FROM `%s`;",
	Oracle:      "INSERT INTO %s SELECT * FROM %s;",
	CockroachDB: "INSERT INTO \"%s\" SELECT * FROM \"%s\";",
}

func getQueryForDuplicateTableInsert(databaseType DatabaseType) (string, error) {
	template, ok := duplicateInsertDataQueryTemplates[databaseType]
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}
	return template, nil
}

type DatabaseInfo struct {
	PlaceholderFormat func(int) string
	QuoteChar         string
}

var dbInfoMap = map[DatabaseType]DatabaseInfo{
	MySQL:       {PlaceholderFormat: func(i int) string { return "?" }, QuoteChar: "`"},
	MariaDB:     {PlaceholderFormat: func(i int) string { return "?" }, QuoteChar: "`"},
	SQLite:      {PlaceholderFormat: func(i int) string { return "?" }, QuoteChar: "\""},
	CockroachDB: {PlaceholderFormat: func(i int) string { return "?" }, QuoteChar: "\""},
	PostgreSQL:  {PlaceholderFormat: func(i int) string { return fmt.Sprintf("$%d", i) }, QuoteChar: "\""},
	SQLServer:   {PlaceholderFormat: func(i int) string { return fmt.Sprintf("@p%d", i) }, QuoteChar: "\""},
	Oracle:      {PlaceholderFormat: func(i int) string { return fmt.Sprintf(":%d", i) }, QuoteChar: "\""},
}

func getQueryPlaceholder(driverName DatabaseType) (string, error) {
	switch driverName {
	case MySQL, MariaDB, SQLite:
		return "?", nil
	case PostgreSQL, CockroachDB:
		return "$1", nil
	case SQLServer:
		return "@p1", nil
	case Oracle:
		return ":1", nil
	default:
		return "", fmt.Errorf("unsupported driver %s", driverName)
	}
}

func getInsertQueryPlaceholders(values []interface{}, databaseType DatabaseType) (string, error) {
	var placeholders string

	switch databaseType {
	case MySQL, MariaDB:
		placeholders = strings.Repeat("?, ", len(values)-1) + "?"
	case PostgreSQL:
		placeholders = strings.Repeat("$", len(values))
		for i := 1; i < len(values); i++ {
			placeholders += ", $" + fmt.Sprint(i+1)
		}
	case SQLite:
		placeholders = strings.Repeat("?, ", len(values)-1) + "?"
	case SQLServer:
		placeholders = strings.Repeat("@p%d, ", len(values)-1) + "@p%d"
	case Oracle:
		placeholders = strings.Repeat(":1, ", len(values)-1) + ":1"
	case CockroachDB:
		placeholders = strings.Repeat("$", len(values))
		for i := 1; i < len(values); i++ {
			placeholders += ", $" + fmt.Sprint(i+1)
		}
	default:
		return "", fmt.Errorf("AddRecord: unsupported driver %s", databaseType)
	}

	return placeholders, nil
}

func getConnectionString(connInfo *SQLConnectionInfo) (string, error) {
	switch connInfo.Type {
	case PostgreSQL:
		return fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			connInfo.Host, connInfo.Port, connInfo.User, connInfo.Passwd, connInfo.DBName,
		), nil
	case MySQL:
		return fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s",
			connInfo.User, connInfo.Passwd, connInfo.Host, connInfo.Port, connInfo.DBName,
		), nil
	case SQLite:
		return fmt.Sprintf("file:%s?cache=shared&mode=rwc", connInfo.Host), nil
	case SQLServer:
		return fmt.Sprintf(
			"server=%s;user id=%s;password=%s;database=%s",
			connInfo.Host, connInfo.User, connInfo.Passwd, connInfo.DBName,
		), nil
	case Oracle:
		return fmt.Sprintf(
			"user=%s;password=%s;connectString=%s/%s",
			connInfo.User, connInfo.Passwd, connInfo.Host, connInfo.DBName,
		), nil
	case CockroachDB:
		return fmt.Sprintf(
			"postgresql://%s:%s@%s:%s/%s?sslmode=disable",
			connInfo.User, connInfo.Passwd, connInfo.Host, connInfo.Port, connInfo.DBName,
		), nil
	default:
		return "", fmt.Errorf("unsupported database type: %s", connInfo.Type)
	}
}
