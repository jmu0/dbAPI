package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/jmu0/dbAPI/db"
)

func handleYaml() {
	var bytes []byte
	var err error
	var tbl db.Table
	var schema db.Schema
	var d db.Database
	var databaseName string
	if _, ok := s["table"]; ok {
		tbl, err = db.GetTable(s["schema"], s["table"], connect())
		if err != nil {
			log.Fatal("GetTable:", err)
		}
		bytes, err = db.Table2Yaml(&tbl)
		if err != nil {
			log.Fatal("Table2Yamle:", err)
		}
	} else if _, ok := s["schema"]; ok {
		schema, err = db.GetSchema(s["schema"], connect())
		if err != nil {
			log.Fatal("GetSchema:", err)
		}
		bytes, err = db.Schema2Yaml(&schema)
		if err != nil {
			log.Fatal("Schema2Yaml:", err)
		}
	} else {
		databaseName = "database"
		if _, ok := s["database"]; ok {
			databaseName = s["database"]
		}
		d, err = db.GetDatabase(databaseName, connect())
		if err != nil {
			log.Fatal("GetDatabase:", err)
		}
		bytes, err = db.Database2Yaml(&d)
		if err != nil {
			log.Fatal("Database2Yaml:", err)
		}
	}
	fmt.Println(string(bytes))
}

func handleSQL() {
	var file string
	var sql, pre, post, fileType string
	var err error
	var schema db.Schema
	var d db.Database
	var tbl db.Table
	var conn db.Conn
	var bytes []byte

	bytes, err = readStdIn()
	if err != nil {
		file = ask("file")
		bytes, err = ioutil.ReadFile(file)
		if err != nil {
			log.Fatal(err)
		}
	}
	fileType, err = getType(bytes)
	if err != nil {
		log.Fatal(err)
	}

	conn = connect()
	if fileType == "table" {
		tbl, err = db.Yaml2Table(bytes)
		if err != nil {
			log.Fatal(err)
		}
		sql, err = db.UpdateTableSQL(&tbl, conn, true)
		if err != nil {
			log.Fatal(err)
		}
	} else if fileType == "schema" {
		schema, err = db.Yaml2Schema(bytes)
		if err != nil {
			log.Fatal(err)
		}
		sql, err = db.UpdateSchemaSQL(&schema, conn)
		if err != nil {
			log.Fatal(err)
		}
	} else if fileType == "database" {
		d, err = db.Yaml2Database(bytes)
		if err != nil {
			log.Fatal(err)
		}
		sql, err = db.UpdateDatabaseSQL(&d, conn)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("Invalid yaml file:" + file)
	}
	pre = conn.PreSQL()
	if len(pre) > 0 {
		pre += "\n"
	}
	if len(sql) > 0 {
		sql += "\n"
	}
	post = conn.PostSQL()
	if len(sql) > 0 {
		sql = pre + sql + post
		fmt.Println(sql)
	}
}

func getType(bytes []byte) (string, error) {
	if len(bytes) < 10 {
		return "", errors.New("Invalid yaml")
	}
	spl := strings.Split(string(bytes[:10]), "_")
	if spl[0] == "table" {
		return "table", nil
	} else if spl[0] == "schema" {
		return "schema", nil
	} else if spl[0] == "database" {
		return "database", nil
	}
	return "", errors.New("Invalid yaml")
}
