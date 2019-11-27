package db

import (
	"database/sql"
)

//Conn interface
type Conn interface {
	Connect(args map[string]string) error
	GetConnection() *sql.DB
	GetSchemaNames() ([]string, error)
	GetTableNames(schemaName string) ([]string, error)
	GetRelationships(schemaName string, tableName string) ([]Relationship, error)
	GetColumns(schemaName, tableName string) ([]Column, error)
}

//Ddl interface (data definition stuff)
type Ddl interface {
	Connect(args map[string]string) error
	GetConnection() *sql.DB
	CreateTableSQL(tbl *Table) (string, error)
	DropTableSQL(tbl *Table) (string, error)
	CreateSchemaSQL(schemaName string) (string, error)
	DropSchemaSQL(schemaName string) (string, error)
}

//Column holds column data
type Column struct {
	Name          string
	Type          string
	Length        int
	Nullable      bool
	PrimaryKey    bool
	AutoIncrement bool
	DefaultValue  string
	Value         interface{}
}

//Relationship between tables
type Relationship struct {
	FromTable   string
	FromCols    string
	ToTable     string
	ToCols      string
	Cardinality string
}

//Table struct
type Table struct {
	Name          string
	Schema        string
	Columns       []Column
	Relationships []Relationship
}

//Schema struct
type Schema struct {
	Name   string
	Tables []Table
}
