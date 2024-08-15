package sqlutils

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
)

func ConnectToMySQLDB(connect SQLConnectionInfo) (*sql.DB, error) {
	cfg := mysql.Config{
		User:   connect.User,
		Passwd: connect.Passwd,
		Net:    "tcp",
		Addr:   connect.Addr,
		DBName: connect.DBName,
	}

	var err error
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("ConnectToMySQLDB - opening db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("ConnectToMySQLDB - pinging db: %w", err)
	}

	return db, nil
}

func GetTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
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

func GetTable(db *sql.DB, tableName string) (interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM `%s`;", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetTable - query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("GetTable - retrieving columns: %w", err)
	}

	sliceType := reflect.SliceOf(reflect.StructOf(createFields(columns)))
	sliceValue := reflect.MakeSlice(sliceType, 0, 0)

	for rows.Next() {
		elemValue := reflect.New(sliceType.Elem()).Elem()

		var fields []interface{}
		for i := 0; i < elemValue.NumField(); i++ {
			fields = append(fields, elemValue.Field(i).Addr().Interface())
		}

		if err := rows.Scan(fields...); err != nil {
			return nil, fmt.Errorf("GetTable - scanning row: %w", err)
		}

		sliceValue = reflect.Append(sliceValue, elemValue)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetTable - rows iteration: %w", err)
	}

	return sliceValue.Interface(), nil
}

func GetColumns(db *sql.DB, tableName string) ([]string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE()
		ORDER BY ORDINAL_POSITION;
	`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("GetColumns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("GetColumns: %v", err)
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetColumns: %v", err)
	}

	return columns, nil
}

func AddRecord(db *sql.DB, tableName string, columns []string, values []interface{}) (int64, error) {
	if len(columns) == 0 || len(values) == 0 || len(columns) != len(values) {
		return 0, fmt.Errorf("AddRecord: invalid columns or values length")
	}

	placeholders := strings.Repeat("?, ", len(values)-1) + "?"

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		placeholders,
	)

	result, err := db.Exec(query, values...)
	if err != nil {
		return 0, fmt.Errorf("AddRecord: %v", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("AddRecord: %v", err)
	}

	return id, nil
}

func EditRecord(
	db *sql.DB,
	tableName string,
	recordIdColumn string,
	recordIdValue string,
	updateColumn string,
	updateValue any,
) error {
	query := fmt.Sprintf("UPDATE `%s` SET %s = ?  WHERE %s = ?", tableName, updateColumn, recordIdColumn)

	result, err := db.Exec(query, updateValue, recordIdValue)
	if err != nil {
		return fmt.Errorf("EditRecord: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("EditRecord - could not get rows affected: %v", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("EditRecord - no rows were updated")
	}

	return nil
}

func RemoveRecord(db *sql.DB, tableName string, columns []string, values []interface{}) (int64, error) {
	if len(columns) != len(values) {
		return 0, fmt.Errorf("RemoveRecord: columns and values length mismatch")
	}
	if len(columns) == 0 {
		return 0, fmt.Errorf("RemoveRecord: columns array cannot be empty")
	}

	var conditions []string
	for _, col := range columns {
		conditions = append(conditions, fmt.Sprintf("%s = ?", col))
	}

	query := fmt.Sprintf("DELETE FROM `%s` WHERE %s", tableName, strings.Join(conditions, " AND "))

	result, err := db.Exec(query, values...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

func GetPrimaryKeys(db *sql.DB, dbName, tableName string) ([]string, error) {
	query := `
		SELECT COLUMN_NAME 
		FROM information_schema.KEY_COLUMN_USAGE 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION;
	`

	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("GetPrimaryKeys: failed to execute query: %w", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("GetPrimaryKeys: failed to scan row: %w", err)
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetPrimaryKeys: rows iteration error: %w", err)
	}

	return primaryKeys, nil
}

func DuplicateTable(db *sql.DB, originalTableName, newTableName string) error {
	validName := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	if !validName.MatchString(originalTableName) || !validName.MatchString(newTableName) {
		return fmt.Errorf("DuplicateTable: table names must contain only letters, numbers, underscores, and dashes")
	}

	createTableQuery := fmt.Sprintf("CREATE TABLE `%s` LIKE `%s`;", newTableName, originalTableName)
	_, err := db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("DuplicateTable: failed to create table structure: %v", err)
	}

	insertDataQuery := fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`;", newTableName, originalTableName)
	_, err = db.Exec(insertDataQuery)
	if err != nil {
		return fmt.Errorf("DuplicateTable: failed to insert data into new table: %v", err)
	}

	return nil
}

func DeleteTable(db *sql.DB, tableName string) error {
	query := fmt.Sprintf("DROP TABLE `%s`", tableName)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("DeleteTable: failed to delete table %s: %v", tableName, err)
	}

	return nil
}

func RenameTable(db *sql.DB, oldTableName string, newTableName string) error {
	query := fmt.Sprintf("RENAME TABLE `%s` TO `%s`;", oldTableName, newTableName)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("RenameTable: could not rename table: %v", err)
	}

	return nil
}

func createFields(columns []string) []reflect.StructField {
	var fields []reflect.StructField
	for _, col := range columns {
		fields = append(fields, reflect.StructField{
			Name: col,
			Type: reflect.TypeOf(""),
		})
	}
	return fields
}
