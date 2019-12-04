package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/jmu0/dbAPI/db"
	"github.com/jmu0/dbAPI/db/mysql"
	"github.com/jmu0/dbAPI/db/postgresql"
	"github.com/jmu0/settings"
)

var conn *sql.DB
var err error
var s map[string]string
var cols []db.Column

func main() {
	s = make(map[string]string)
	settings.Load("dbbuild.conf", &s)
	if len(os.Args) == 1 {
		printHelp()
		return
	}
	switch os.Args[1] {
	case "html":
		handleHTML()
	case "template":
		handleTemplate()
	case "yaml":
		handleYaml()
	case "sql":
		handleSQL()
	default:
		printHelp()

	}
}

func connect() db.Conn {
	var err error
	var tp, host, user, pwd string
	tp = ask("driver")
	host = ask("hostname")
	user = ask("username")
	pwd = ask("password")
	if tp == "mysql" {
		c := mysql.Conn{}
		err = c.Connect(map[string]string{
			"hostname": host,
			"username": user,
			"password": pwd,
		})
		if err != nil {
			log.Fatal(err)
		}
		return &c
	} else if tp == "postgresql" {
		var database string
		database = ask("database")
		c := postgresql.Conn{}
		err = c.Connect(map[string]string{
			"hostname": host,
			"username": user,
			"password": pwd,
			"database": database,
		})
		if err != nil {
			log.Fatal(err)
		}
		return &c
	} else {
		fmt.Println("invalid driver: " + tp)
		os.Exit(0)
	}
	return nil
}
func printHelp() {
	fmt.Println(`Usage:
dbbuild html
  Reads table structure from database and builds html table
  env/clo: driver, hostname, database, username, password, schema, table
dbbuild template
  Reads table structure from database and builds html template
  env/clo: driver, hostname, database, username, password, schema, table
dbbuild yaml
  Reads table structure from database and builds yaml
  env/clo: driver, hostname, database, username, password, schema, [table]
dbbuild sql
  Creates sql to create/modify database from yaml file
  env/clo: driver, hostname, database, username, password, file
  reads yaml from file (--file=..) or stdin`)
}

func ask(key string) string {
	if s, ok := s[key]; ok {
		return s
	}
	var ret string
	fmt.Print("(" + os.Args[0] + ") " + key + ": ")
	fmt.Scanln(&ret)
	return ret
}

func readStdIn() ([]byte, error) {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return []byte{}, err
	}
	if fi.Mode()&os.ModeNamedPipe == 0 {
		return []byte{}, errors.New("No pipe")
	}
	return ioutil.ReadAll(os.Stdin)
}
