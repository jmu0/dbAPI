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
	PreSQL() string  //sql to put at start of ddl query
	PostSQL() string //sql to put at end of ddl query
	CreateTableSQL(tbl *Table) (string, error)
	DropTableSQL(tbl *Table) (string, error)
	CreateSchemaSQL(schemaName string) (string, error)
	DropSchemaSQL(schemaName string) (string, error)
}

//Column holds column data
type Column struct {
	Name          string      `json:"name" yaml:"name"`
	Type          string      `json:"type,omitempty" yaml:"type,omitempty"`
	Length        int         `json:"length,omitempty" yaml:"length,omitempty"`
	Nullable      bool        `json:"nullable,omitempty" yaml:"nullable,omitempty"`
	PrimaryKey    bool        `json:"primarykey,omitempty" yaml:"primarykey,omitempty"`
	AutoIncrement bool        `json:"autoincrement,omitempty" yaml:"autoincrement,omitempty"`
	DefaultValue  string      `json:"default,omitempty" yaml:"default,omitempty"`
	Value         interface{} `json:"-" yaml:"-"`
}

//Relationship between tables
type Relationship struct {
	FromTable   string
	FromCols    string
	ToTable     string
	ToCols      string
	Cardinality string
}

//ForeignKey like only many-to-one relationships
type ForeignKey struct {
	FromCols string `json:"fromcols" yaml:"fromcols"`
	ToTable  string `json:"totable" yaml:"totable"`
	ToCols   string `json:"tocols" yaml:"tocols"`
}

//Table struct
type Table struct {
	Name        string       `json:"table_name" yaml:"table_name"`
	Schema      string       `json:"schema" yaml:"schema"`
	Columns     []Column     `json:"columns" yaml:"columns"`
	ForeignKeys []ForeignKey `json:"foreign_keys,omitempty" yaml:"foreign_keys,omitempty"`
}

//Schema struct
type Schema struct {
	Name   string  `json:"schema_name" yaml:"schema_name"`
	Tables []Table `json:"tables" yaml:"tables"`
}
