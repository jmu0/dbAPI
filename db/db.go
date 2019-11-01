package db

import "database/sql"

//Conn interface
type Conn interface {
	Connect(args map[string]string) error
	GetConnection() *sql.DB
	GetSchemaNames() ([]string, error)
	GetTableNames(databaseName string) ([]string, error)
	GetRelationships(databaseName string, tableName string) ([]Relationship, error)
	GetColumns(databaseName, tableName string) ([]Column, error)
}

//Column holds column data
type Column struct {
	Name         string
	Type         string
	Length       int
	Nullable     bool
	PrimaryKey   bool
	DefaultValue string
	Value        interface{}
}

//Relationship between tables
type Relationship struct {
	FromTable   string
	FromCols    string
	ToTable     string
	ToCols      string
	Cardinality string
}

//Query queries the database
func Query(c Conn, query string) ([]map[string]interface{}, error) {
	res := make([]map[string]interface{}, 0)
	rows, err := c.GetConnection().Query(query)
	if err != nil {
		return res, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return res, err
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		rows.Scan(scanArgs...)
		v := make(map[string]interface{})
		var value interface{}
		for i, col := range values {
			if col == nil {
				value = ""
			} else {
				value = string(col)
			}
			v[columns[i]] = value
		}
		res = append(res, v)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return res, err
	}
	return res, nil
}

//Execute executes query without returning results. returns (lastInsertId, rowsAffected, error)
func Execute(c Conn, query string, params []interface{}) (int64, int64, error) {
	res, err := c.GetConnection().Exec(query, params)
	if err != nil {
		return 0, 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	return id, n, nil
}
