package sqlutils

import (
	"database/sql"
	"fmt"
	"strings"
)

// Extracts the keys and the values from the record
func extractRecordData(record TableRecord) ([]string, []interface{}) {
	var recordKeys = make([]string, len(record))
	var recordValues = make([]interface{}, len(record))
	var index = 0

	for key, value := range record {
		recordKeys[index] = key
		recordValues[index] = value
		index += 1
	}

	return recordKeys, recordValues
}

// Example return: order_id = ? AND customer_number = ?
func computeConditions(keys []string, databaseType DatabaseType, placeholder string) string {
	conditions := make([]string, len(keys))
	for index, key := range keys {
		local_placeholder := placeholder

		switch databaseType {
		case PostgreSQL, SQLServer, Oracle:
			local_placeholder = fmt.Sprintf("%s%d", local_placeholder, index)
		}

		conditions[index] = fmt.Sprintf("%s = %s", key, local_placeholder)
	}

	return strings.Join(conditions, " AND ")
}

func InsertRecord(
	db *sql.DB,
	tableName string,
	record TableRecord,
	databaseType DatabaseType,
) (int64, error) {
	recordKeys, recordValues := extractRecordData(record)

	quoteChar, ok := quoteCharMap[databaseType]
	if !ok {
		return 0, fmt.Errorf("%s - database type not supported", getCurrentFuncName())
	}
	placeholder, ok := placeholderMap[databaseType]
	if !ok {
		return 0, fmt.Errorf("%s - database type not supported", getCurrentFuncName())
	}

	repeatedPlaceholder := strings.Repeat(placeholder, len(recordValues))
	repeatedPlaceholderArray := strings.Split(repeatedPlaceholder, "")

	switch databaseType {
	case PostgreSQL, SQLServer, Oracle:
		for index := range repeatedPlaceholderArray {
			repeatedPlaceholderArray[index] = fmt.Sprintf("%s%d", repeatedPlaceholderArray[index], index)
		}
	}

	placeholders := strings.Join(repeatedPlaceholderArray, ", ")

	query := fmt.Sprintf("INSERT INTO %s%s%s (%s) VALUES (%s)",
		quoteChar, tableName, quoteChar,
		strings.Join(recordKeys, ", "),
		placeholders,
	)

	result, err := db.Exec(query, recordValues...)
	if err != nil {
		return 0, fmt.Errorf("%s - %v", getCurrentFuncName(), err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("%s - %v", getCurrentFuncName(), err)
	}

	return id, nil
}

func DuplicateRecord(
	db *sql.DB,
	dbName string,
	tableName string,
	record TableRecord,
	databaseType DatabaseType,
) error {
	primaryKeys, err := GetPrimaryKeys(db, dbName, tableName, databaseType)
	if err != nil {
		return fmt.Errorf("%s - error grabbing primary keys: %w", getCurrentFuncName(), err)
	}

	columnTypes, err := getColumnTypes(db, dbName, tableName, databaseType)
	if err != nil {
		return fmt.Errorf("%s - error grabbing column types: %w", getCurrentFuncName(), err)
	}

	for _, key := range primaryKeys {
		dataType := columnTypes[key]
		record[key] = generateNewPrimaryKeyValue(dataType)
	}

	_, err = InsertRecord(db, tableName, record, databaseType)
	if err != nil {
		return fmt.Errorf("%s - error inserting record: %w", getCurrentFuncName(), err)
	}

	return nil
}

func EditRecord(
	db *sql.DB,
	tableName string,
	record TableRecord,
	updateColumn string,
	updateValue any,
	databaseType DatabaseType,
) error {
	quoteChar, ok := quoteCharMap[databaseType]
	if !ok {
		return fmt.Errorf("%s - database type not supported", getCurrentFuncName())
	}
	placeholder, ok := placeholderMap[databaseType]
	if !ok {
		return fmt.Errorf("%s - database type not supported", getCurrentFuncName())
	}

	// also add identify by primary key like when removing

	switch databaseType {
	case PostgreSQL, SQLServer, Oracle:
		placeholder = fmt.Sprintf("%s%d", placeholder, 1)
	}

	recordKeys, recordValues := extractRecordData(record)
	conditions := computeConditions(recordKeys, databaseType, placeholder)

	query := fmt.Sprintf("UPDATE %s%s%s SET %s = %s WHERE %s",
		quoteChar, tableName, quoteChar,
		updateColumn, placeholder,
		conditions,
	)

	args := append([]interface{}{updateValue}, recordValues...)

	result, err := db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("%s - %v", getCurrentFuncName(), err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s - could not get rows affected - %v", getCurrentFuncName(), err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("%s - no rows were updated", getCurrentFuncName())
	}

	return nil
}

func RemoveRecord(
	db *sql.DB,
	dbName,
	tableName string,
	databaseType DatabaseType,
	record TableRecord,
) (int64, error) {
	primaryKeys, err := GetPrimaryKeys(db, dbName, tableName, databaseType)
	if err != nil {
		return 0, fmt.Errorf("%s - error grabbing primary keys: %w", getCurrentFuncName(), err)
	}

	quoteChar, ok := quoteCharMap[databaseType]
	if !ok {
		return 0, fmt.Errorf("%s - database type not supported", getCurrentFuncName())
	}
	placeholder, ok := placeholderMap[databaseType]
	if !ok {
		return 0, fmt.Errorf("%s - database type not supported", getCurrentFuncName())
	}

	// remove by primary key if any available
	if len(primaryKeys) != 0 {
		firstPrimaryKey := primaryKeys[0]

		primaryKeyValue, ok := record[firstPrimaryKey]
		if !ok {
			return 0, fmt.Errorf("%s - primary key not provided", getCurrentFuncName())
		}

		switch databaseType {
		case PostgreSQL, SQLServer, Oracle:
			placeholder = fmt.Sprintf("%s%d", placeholder, 1)
		}

		query := fmt.Sprintf("DELETE FROM %s%s%s WHERE %s%s%s = %s",
			quoteChar, tableName, quoteChar,
			quoteChar, firstPrimaryKey, quoteChar,
			placeholder,
		)

		result, err := db.Exec(query, primaryKeyValue)
		if err != nil {
			return 0, err
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}

		if rowsAffected == 0 {
			return 0, fmt.Errorf("%s - record does not exist", getCurrentFuncName())
		}

		return rowsAffected, nil
	}

	recordKeys, recordValues := extractRecordData(record)
	conditions := computeConditions(recordKeys, databaseType, placeholder)

	query := fmt.Sprintf("DELETE FROM %s%s%s WHERE %s",
		quoteChar, tableName, quoteChar,
		conditions,
	)

	stmt, err := db.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(recordValues...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}
