package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/jmu0/dbAPI/db"
)

func handleYaml() {

	if _, ok := s["schema"]; !ok {
		log.Fatal("No schema")
	}
	var bytes []byte
	var err error
	var tbl db.Table
	var schema db.Schema
	if _, ok := s["table"]; ok {
		tbl, err = db.GetTable(s["schema"], s["table"], connect())
		if err != nil {
			log.Fatal("GetTable:", err)
		}
		bytes, err = db.Table2Yaml(&tbl)
		if err != nil {
			log.Fatal("Table2Yamle:", err)
		}
	} else {
		schema, err = db.GetSchema(s["schema"], connect())
		if err != nil {
			log.Fatal("GetSchema:", err)
		}
		bytes, err = db.Schema2Yaml(&schema)
		if err != nil {
			log.Fatal("Schema2Yaml:", err)
		}
	}
	fmt.Println(string(bytes))
}

func handleSQL() {
	var file = ask("file")
	var sql, pre, post, fileType string
	var err error
	var schema db.Schema
	var tbl db.Table
	var conn db.Conn
	var bytes []byte

	fileType, err = getFileType(file)
	if err != nil {
		log.Fatal(err)
	}
	bytes, err = ioutil.ReadFile(file)
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
	sql = pre + sql + post
	fmt.Println(sql)
}

func getFileType(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return "", err
	}
	firstline := scanner.Text()
	spl := strings.Split(firstline, "_")
	if spl[0] == "table" {
		return "table", nil
	} else if spl[0] == "schema" {
		return "schema", nil
	}
	return "", errors.New("Invalid yaml file:" + filename)
}
