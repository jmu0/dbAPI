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
	GetIndexes(schemaName, tableName string) ([]Index, error)
	Quote(str string) string
	PreSQL() string  //sql to put at start of ddl query
	PostSQL() string //sql to put at end of ddl query
	CreateTableSQL(tbl *Table) (string, error)
	DropTableSQL(tbl *Table) string
	CreateSchemaSQL(schemaName string) string
	DropSchemaSQL(schemaName string) string
	CreateIndexSQL(schemaName, tableName string, index *Index) string
	DropIndexSQL(schemaName, tableName, indexName string) string
	AddColumnSQL(schemaName, tableName string, col *Column) (string, error)
	DropColumnSQL(schemaName, tableName, columnName string) string
	AlterColumnSQL(schemaName, tableName string, col *Column) (string, error)
	AddForeignKeySQL(schemaName, tableName string, fk *ForeignKey) string
	DropForeignKeySQL(schemaName, tableName, keyName string) string
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
	Name        string
	FromTable   string
	FromCols    string
	ToTable     string
	ToCols      string
	Cardinality string
}

//ForeignKey only many-to-one relationships
type ForeignKey struct {
	Name     string `json:"name,omitempty" yaml:"name,omitempty"`
	FromCols string `json:"fromcols" yaml:"fromcols"`
	ToTable  string `json:"totable" yaml:"totable"`
	ToCols   string `json:"tocols" yaml:"tocols"`
}

//Index on table
type Index struct {
	Name    string `json:"name,omitempty" yaml:"name,omitempty"`
	Columns string `json:"columns" yaml:"columns"`
}

//Table struct
type Table struct {
	Name        string       `json:"table_name" yaml:"table_name"`
	Schema      string       `json:"schema" yaml:"schema"`
	Columns     []Column     `json:"columns" yaml:"columns"`
	ForeignKeys []ForeignKey `json:"foreign_keys,omitempty" yaml:"foreign_keys,omitempty"`
	Indexes     []Index      `json:"indexes,omitempty" yaml:"indexes,omitempty"`
}

//Schema struct
type Schema struct {
	Name   string  `json:"schema_name" yaml:"schema_name"`
	Tables []Table `json:"tables" yaml:"tables"`
}

//Database struct
type Database struct {
	Name    string   `json:"database_name" yaml:"database_name"`
	Schemas []Schema `json:"schemas" yaml:"schemas"`
}
