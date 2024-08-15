package sqlutils

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
)

func Test() {
	fmt.Println("testing golang packages")
}

func ConnectToDb(connect SQLConnectionInfo) (*sql.DB, error) {
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
		return nil, fmt.Errorf("connectToDb - opening db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("connectToDb - pinging db: %w", err)
	}

	return db, nil
}

func GetTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("getTables - fetching tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("getTables - scanning row: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}
	return tableNames, rows.Err()
}

func GetTable(db *sql.DB, tableName string) (interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM `%s`;", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("getTable - query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getTable - retrieving columns: %w", err)
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
			return nil, fmt.Errorf("getTable - scanning row: %w", err)
		}

		sliceValue = reflect.Append(sliceValue, elemValue)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getTable - rows iteration: %w", err)
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
		return nil, fmt.Errorf("getColumns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("getColumns: %v", err)
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getColumns: %v", err)
	}

	return columns, nil
}

func AddRecord(db *sql.DB, tableName string, columns []string, values []interface{}) (int64, error) {
	if len(columns) == 0 || len(values) == 0 || len(columns) != len(values) {
		return 0, fmt.Errorf("addRecord: invalid columns or values length")
	}

	placeholders := strings.Repeat("?, ", len(values)-1) + "?"

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		placeholders,
	)

	result, err := db.Exec(query, values...)
	if err != nil {
		return 0, fmt.Errorf("addRecord: %v", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("addRecord: %v", err)
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
		return fmt.Errorf("editRecord: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("editRecord - could not get rows affected: %v", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("editRecord - no rows were updated")
	}

	return nil
}

func RemoveRecord(db *sql.DB, tableName string, columns []string, values []interface{}) (int64, error) {
	if len(columns) != len(values) {
		return 0, fmt.Errorf("removeRecord: columns and values length mismatch")
	}
	if len(columns) == 0 {
		return 0, fmt.Errorf("removeRecord: columns array cannot be empty")
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
		return nil, fmt.Errorf("getPrimaryKeys: failed to execute query: %w", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("getPrimaryKeys: failed to scan row: %w", err)
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getPrimaryKeys: rows iteration error: %w", err)
	}

	return primaryKeys, nil
}

func DuplicateTable(db *sql.DB, originalTableName, newTableName string) error {
	validName := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	if !validName.MatchString(originalTableName) || !validName.MatchString(newTableName) {
		return fmt.Errorf("duplicateTable: table names must contain only letters, numbers, underscores, and dashes")
	}

	createTableQuery := fmt.Sprintf("CREATE TABLE `%s` LIKE `%s`;", newTableName, originalTableName)
	_, err := db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("duplicateTable: failed to create table structure: %v", err)
	}

	insertDataQuery := fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`;", newTableName, originalTableName)
	_, err = db.Exec(insertDataQuery)
	if err != nil {
		return fmt.Errorf("duplicateTable: failed to insert data into new table: %v", err)
	}

	return nil
}

func DeleteTable(db *sql.DB, tableName string) error {
	query := fmt.Sprintf("DROP TABLE `%s`", tableName)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("deleteTable: failed to delete table %s: %v", tableName, err)
	}

	return nil
}

func RenameTable(db *sql.DB, oldTableName string, newTableName string) error {
	query := fmt.Sprintf("RENAME TABLE `%s` TO `%s`;", oldTableName, newTableName)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("renameTable: could not rename table: %v", err)
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
