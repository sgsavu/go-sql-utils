package sqlutils

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"reflect"
	"unicode"
)

func sGetTable(db *sql.DB, tableName string, dbType DatabaseType) (interface{}, error) {
	query, err := getQueryForAllRecords(tableName, dbType)
	if err != nil {
		return nil, fmt.Errorf("GetTables - grabbing db type specific query: %w", err)
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

func createFields(columns []string) []reflect.StructField {
	var fields []reflect.StructField
	for _, column := range columns {
		// Capitalize the first letter to make it exported
		runes := []rune(column)
		runes[0] = unicode.ToUpper(runes[0])
		capitalizedName := string(runes)

		fields = append(fields, reflect.StructField{
			Name: capitalizedName,
			Type: reflect.TypeOf(""), // Assuming all fields are of type string;
			Tag:  reflect.StructTag(`json:"` + column + `"`),
		})
	}
	return fields
}

// Helper function to check if a string is base64 encoded
func isBase64(b []byte) bool {
	return len(b) > 0 && (b[len(b)-1] == '=' || b[len(b)-1] == '/')
}

// Helper function to decode base64 encoded strings
func decodeBase64(b []byte) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func findIndex(slice []string, item string) int {
	for i, v := range slice {
		if v == item {
			return i
		}
	}
	return -1
}
