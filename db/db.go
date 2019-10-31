package db

//Conn interface
type Conn interface {
	Connect(args map[string]string) error
	GetSchemaNames() ([]string, error)
	GetTableNames(databaseName string) ([]string, error)
	GetRelationships(databaseName string, tableName string) ([]Relationship, error)
	GetColumns(databaseName, tableName string) ([]Column, error)
}

//Column holds column data
type Column struct {
	Field        string
	Type         string
	Length       int
	Nullable     bool
	PrimaryKey   bool
	DefaultValue string
	Extra        string
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
func Query(db Conn, query string) ([]map[string]interface{}, error) {
	return nil, nil
}
