package main

import (
	"errors"
	"io/ioutil"
	"log"

	"github.com/jmu0/dbAPI/db"
)

//handleDump dumps data from database into .csv file
func handleDump() {
	var conn = connect()
	if _, ok := s["table"]; ok {
		if _, ok := s["schema"]; !ok {
			log.Fatal("No schema")
		}
		tbl, err := db.GetTable(s["schema"], s["table"], conn)
		if err != nil {
			log.Fatal(err)
		}
		err = table2csv(&tbl, conn)
		if err != nil {
			log.Fatal(err)
		}
	} else if _, ok := s["schema"]; ok {
		schema, err := db.GetSchema(s["schema"], conn)
		if err != nil {
			log.Fatal(err)
		}
		for _, tbl := range schema.Tables {
			err = table2csv(&tbl, conn)
		}
	} else {

	}
}

//handleLoad loads data from .csv file into table
func handleLoad() {
	//TODO: handle load data from .csv file
}

func table2csv(tbl *db.Table, conn db.Conn) error {
	bytes, err := db.DumpTable(tbl, conn)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return errors.New("No data")
	}
	filename := tbl.Schema + "." + tbl.Name + ".data.csv"
	return ioutil.WriteFile(filename, bytes, 0770)
}
