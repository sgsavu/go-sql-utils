package sqlutils

import (
	"database/sql"
	"fmt"
	"time"
)

func doesTableExist(db *sql.DB, tableName string, dbType DatabaseType) error {
	quoteChar, ok := quoteCharMap[dbType]
	if !ok {
		return fmt.Errorf("%s - database type not supported", getCurrentFuncName())
	}

	query := fmt.Sprintf("SELECT 1 FROM %s%s%s", quoteChar, tableName, quoteChar)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("IsTableExistent - query: %w", err)
	}
	rows.Close()

	return nil
}

func GetTables(db *sql.DB, dbName string, dbType DatabaseType) ([]string, error) {
	query, err := getQueryForTables(dbName, dbType)
	if err != nil {
		return nil, fmt.Errorf("GetTables - grabbing db type specific query: %w", err)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetTables - fetching tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("GetTables - scanning row: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}
	return tableNames, rows.Err()
}

func GetTable(db *sql.DB, tableName string, dbType DatabaseType) ([]map[string]interface{}, error) {
	query, err := getQueryForAllRecords(tableName, dbType)
	if err != nil {
		return nil, fmt.Errorf("GetTable - grabbing db type specific query: %w", err)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetTable - query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("GetTable - retrieving columns: %w", err)
	}

	results := []map[string]interface{}{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("GetTable - scanning row: %w", err)
		}

		result := make(map[string]interface{})
		for i, col := range columns {
			switch v := values[i].(type) {
			case []byte:
				if isBase64(v) {
					decoded, err := decodeBase64(v)
					if err == nil {
						result[col] = decoded
					} else {
						result[col] = string(v)
					}
				} else {
					result[col] = string(v)
				}
			case time.Time:
				result[col] = v.Format(time.RFC3339)
			default:
				result[col] = v
			}
		}

		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetTable - rows iteration: %w", err)
	}

	return results, nil
}

func GetColumns(db *sql.DB, tableName string, databaseType DatabaseType) ([]string, error) {
	err := doesTableExist(db, tableName, databaseType)
	if err != nil {
		return nil, fmt.Errorf("GetColumns - %w", err)
	}

	query, err := getQueryForColumns(databaseType)
	if err != nil {
		return nil, fmt.Errorf("GetColumns - grabbing db type specific query: %w", err)
	}

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("GetColumns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if databaseType == SQLite {
			var columnInfo struct {
				Name string
			}
			if err := rows.Scan(&columnInfo.Name); err != nil {
				return nil, fmt.Errorf("GetColumns: %v", err)
			}
			columns = append(columns, columnInfo.Name)
		} else {
			if err := rows.Scan(&column); err != nil {
				return nil, fmt.Errorf("GetColumns: %v", err)
			}
			columns = append(columns, column)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetColumns: %v", err)
	}

	return columns, nil
}

func GetPrimaryKeys(db *sql.DB, dbName, tableName string, databaseType DatabaseType) ([]string, error) {
	var err error
	err = doesTableExist(db, tableName, databaseType)
	if err != nil {
		return nil, fmt.Errorf("GetColumns - %w", err)
	}

	query, err := getQueryForPrimaryKeys(databaseType)
	if err != nil {
		return nil, fmt.Errorf("GetPrimaryKeys - grabbing db type specific query: %w", err)
	}

	var rows *sql.Rows

	switch databaseType {
	case SQLite, PostgreSQL:
		rows, err = db.Query(query, tableName)
	default:
		rows, err = db.Query(query, dbName, tableName)
	}

	if err != nil {
		return nil, fmt.Errorf("GetPrimaryKeys: failed to execute query: %w", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if databaseType == SQLite {
			var columnInfo struct {
				Name       string
				NotNull    int
				PrimaryKey int
			}
			if err := rows.Scan(&columnInfo.Name, &columnInfo.NotNull, &columnInfo.PrimaryKey); err != nil {
				return nil, fmt.Errorf("GetPrimaryKeys: failed to scan row: %w", err)
			}
			if columnInfo.PrimaryKey == 1 {
				primaryKeys = append(primaryKeys, columnInfo.Name)
			}
		} else {
			if err := rows.Scan(&columnName); err != nil {
				return nil, fmt.Errorf("GetPrimaryKeys: failed to scan row: %w", err)
			}
			primaryKeys = append(primaryKeys, columnName)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetPrimaryKeys: rows iteration error: %w", err)
	}

	return primaryKeys, nil
}

func DuplicateTable(db *sql.DB, originalTableName, newTableName string, databaseType DatabaseType) error {
	if newTableName != "" && !isValidTableName(newTableName) {
		return fmt.Errorf("DuplicateTable: table names must contain only letters, numbers, underscores, and dashes")
	}

	if newTableName == "" {
		newTableName = fmt.Sprintf("%s-copy-%s", originalTableName, getRandomString(5))
	}

	createTableQuery, err := getQueryForDuplicateTableCreate(databaseType)
	if err != nil {
		return fmt.Errorf("unsupported database type for create table: %s", databaseType)
	}

	createQuery := fmt.Sprintf(createTableQuery, newTableName, originalTableName)
	_, err = db.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("DuplicateTable: failed to create table structure: %v", err)
	}

	insertDataQuery, err := getQueryForDuplicateTableInsert(databaseType)
	if err != nil {
		return fmt.Errorf("unsupported database type for insert data: %s", databaseType)
	}

	insertQuery := fmt.Sprintf(insertDataQuery, newTableName, originalTableName)
	_, err = db.Exec(insertQuery)
	if err != nil {
		return fmt.Errorf("DuplicateTable: failed to insert data into new table: %v", err)
	}

	return nil
}

func DeleteTable(db *sql.DB, tableName string, databaseType DatabaseType) error {
	queryTemplate, err := getQueryForDeleteTable(databaseType)
	if err != nil {
		return fmt.Errorf("DeleteTable - grabbing db type specific query: %w", err)
	}

	query := fmt.Sprintf(queryTemplate, tableName)
	if databaseType == Oracle {
		_, err := db.Exec(query)
		if err != nil {
			return fmt.Errorf("DeleteTable: failed to delete table %s: %v", tableName, err)
		}
		return nil
	}

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("DeleteTable: failed to delete table %s: %v", tableName, err)
	}

	return nil
}

func RenameTable(db *sql.DB, oldTableName string, newTableName string, databaseType DatabaseType) error {
	queryTemplate, err := getQueryForRenameTable(databaseType)
	if err != nil {
		return fmt.Errorf("RenameTable - grabbing db type specific query: %w", err)
	}

	query := fmt.Sprintf(queryTemplate, oldTableName, newTableName)

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("RenameTable: could not rename table from %s to %s: %v", oldTableName, newTableName, err)
	}

	return nil
}
