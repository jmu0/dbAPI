package postgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jmu0/dbAPI/db"
	//connect to postgres
	_ "github.com/lib/pq"
)

//Conn mysql connection implements db.Conn
type Conn struct {
	conn *sql.DB
}

//Connect to database
func (c *Conn) Connect(args map[string]string) error {
	if _, ok := args["hostname"]; !ok {
		return errors.New("No hostname in args")
	}
	if _, ok := args["database"]; !ok {
		return errors.New("No database in args")
	}
	if _, ok := args["username"]; !ok {
		return errors.New("No username in args")
	}
	if _, ok := args["password"]; !ok {
		return errors.New("No password in args")
	}
	if _, ok := args["port"]; !ok {
		args["port"] = "5432"
	}
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=disable",
		args["username"], args["password"], args["database"], args["hostname"])
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return err
	}
	c.conn = db
	return nil
}

//GetSchemaNames from database
func (c *Conn) GetSchemaNames() ([]string, error) {
	dbs := []string{}
	query := "select schema_name from information_schema.schemata"
	rows, err := c.conn.Query(query)
	defer rows.Close()
	if err != nil {
		return dbs, err
	}
	if rows == nil {
		return dbs, errors.New("No databases found")
	}
	dbName := ""
	for rows.Next() {
		rows.Scan(&dbName)
		if !skipDb(dbName) {
			dbs = append(dbs, dbName)
		}
	}
	return dbs, nil
}

//don't show system databases
func skipDb(name string) bool {
	skip := []string{
		"pg_toast",
		"pg_temp_1",
		"pg_toast_temp_1",
		"pg_catalog",
		"information_schema",
	}
	for _, s := range skip {
		if name == s {
			return true
		}
	}
	return false
}

//GetTableNames from database
func (c *Conn) GetTableNames(databaseName string) ([]string, error) {
	tbls := []string{}
	query := "select table_name from information_schema.tables where table_schema='" + strings.ToLower(databaseName) + "'"
	rows, err := c.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows == nil {
		return nil, errors.New("No tables found in " + databaseName)
	}
	tableName := ""
	for rows.Next() {
		rows.Scan(&tableName)
		tbls = append(tbls, tableName)
	}
	return tbls, nil
}

//GetRelationships from database table
func (c *Conn) GetRelationships(databaseName string, tableName string) ([]db.Relationship, error) {
	return nil, nil
}

//GetColumns from database table
func (c *Conn) GetColumns(databaseName, tableName string) ([]db.Column, error) {
	return nil, nil
}

//Query database
func (c *Conn) Query(query string) ([]map[string]interface{}, error) {
	return nil, nil
}
