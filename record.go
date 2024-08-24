package sqlutils

import (
	"database/sql"
	"fmt"
	"strings"
)

func InsertRecord(db *sql.DB, tableName string, columns []string, values []interface{}, databaseType DatabaseType) (int64, error) {
	if len(columns) == 0 || len(values) == 0 || len(columns) != len(values) {
		return 0, fmt.Errorf("AddRecord: invalid columns or values length")
	}

	placeholders, err := getInsertQueryPlaceholders(values, databaseType)
	if err != nil {
		return 0, fmt.Errorf("AddRecord: %v", err)
	}

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
	recordIdValue any,
	updateColumn string,
	updateValue any,
	databaseType DatabaseType,
) error {
	placeholder, err := getQueryPlaceholder(databaseType)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("UPDATE `%s` SET %s = %s WHERE %s = %s",
		tableName,
		updateColumn,
		placeholder,
		recordIdColumn,
		placeholder,
	)

	args := []interface{}{updateValue, recordIdValue}

	result, err := db.Exec(query, args...)
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

// First tries to remove record by primary keys, if no primary keys exist then remove record(s) which meets all values
func RemoveRecord(db *sql.DB, dbName, tableName string, databaseType DatabaseType, values []interface{}) (int64, error) {
	dbInfo, ok := dbInfoMap[databaseType]
	if !ok {
		return 0, fmt.Errorf("RemoveRecord: unsupported database type %s", databaseType)
	}

	primaryKeys, err := GetPrimaryKeys(db, dbName, tableName, databaseType)
	if err != nil {
		return 0, fmt.Errorf("RemoveRecord: error grabbing primary keys: %w", err)
	}

	columns, err := GetColumns(db, tableName, databaseType)
	if err != nil {
		return 0, fmt.Errorf("RemoveRecord: error grabbing table columns: %w", err)
	}

	if len(primaryKeys) != 0 {
		firstPrimaryKey := primaryKeys[0]

		index := findIndex(columns, firstPrimaryKey)

		value := values[index]

		query := fmt.Sprintf("DELETE FROM %s%s%s WHERE %s%s%s = %s",
			dbInfo.QuoteChar, tableName, dbInfo.QuoteChar,
			dbInfo.QuoteChar, firstPrimaryKey, dbInfo.QuoteChar,
			dbInfo.PlaceholderFormat(1),
		)

		result, err := db.Exec(query, value)
		if err != nil {
			return 0, err
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}

		if rowsAffected == 0 {
			return 0, fmt.Errorf("RemoveRecord: non-existent record(s)")
		}

		return rowsAffected, nil
	}

	if len(columns) != len(values) {
		return 0, fmt.Errorf("RemoveRecord: columns and values length mismatch")
	}
	if len(columns) == 0 {
		return 0, fmt.Errorf("RemoveRecord: columns array cannot be empty")
	}

	conditions := make([]string, len(columns))
	for i, col := range columns {
		conditions[i] = fmt.Sprintf("%s = %s", col, dbInfo.PlaceholderFormat(i+1))
	}

	query := fmt.Sprintf("DELETE FROM %s%s%s WHERE %s",
		dbInfo.QuoteChar, tableName, dbInfo.QuoteChar,
		strings.Join(conditions, " AND "),
	)

	stmt, err := db.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(values...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}
