package db

import "database/sql"

type dbConn interface {
	Connect(args map[string]string) (*sql.DB, error)
	GetDatabaseNames() ([]string, error)
	GetTableNames(databaseName string) ([]string, error)
	GetRelationships(databaseName string, tableName string) ([]Relationship, error)
	GetColumns(databaseName, tableName string) ([]Column, error)
	Query(query string) ([]map[string]interface{}, error)
}

//Column holds column data
type Column struct {
	Field    string
	Type     string
	Length   int
	Nullable bool
	Key      string
	Default  string
	Extra    string
	Value    interface{}
}

//Relationship between tables
type Relationship struct {
	FromTable   string
	FromCols    string
	ToTable     string
	ToCols      string
	Cardinality string
}
