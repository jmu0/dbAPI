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
