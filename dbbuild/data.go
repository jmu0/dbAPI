package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"

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
		fmt.Println("Dumping schema: " + schema.Name + "...")
		if err != nil {
			log.Fatal(err)
		}
		for _, tbl := range schema.Tables {
			fmt.Println("-- table: " + tbl.Name + "...")
			err = table2csv(&tbl, conn)
			if err != nil {
				log.Fatal(err)
			}
		}
	} else {
		databaseName := "database"
		if _, ok := s["database"]; ok {
			databaseName = s["database"]
		}
		d, err := db.GetDatabase(databaseName, connect())
		if err != nil {
			log.Fatal("GetDatabase:", err)
		}
		for _, schema := range d.Schemas {
			fmt.Println("Dumping schema: " + schema.Name + "...")
			for _, tbl := range schema.Tables {
				fmt.Println("-- table: " + tbl.Name + "...")
				err = table2csv(&tbl, conn)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}

//handleLoad loads data from .csv file into table
func handleLoad() {
	var fileName string
	var tables []db.Table
	var conn = connect()
	var i int

	if _, ok := s["file"]; ok {
		spl := strings.Split(s["file"], ".")
		if len(spl) != 4 {
			log.Fatal("invalid file: ", s["file"], " len:", len(spl))
		}
		tbl, err := db.GetTable(spl[0], spl[1], conn)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Schema:", tbl.Schema)
		fmt.Println("Table:", tbl.Name)
		if _, ok := s["clear"]; ok {
			err = deleteTableData(tbl, conn)
			if err != nil {
				log.Fatal(err)
			}
		}
		fmt.Print("Loading data from: ", s["file"], "... ")
		err = csv2table(s["file"], tbl, conn)
		if err != nil {
			log.Fatal("csv2table:", err)
		}
	} else {
		files, err := ioutil.ReadDir("./")
		if err != nil {
			log.Fatal(err)
		}
		for _, file := range files {
			fileName = file.Name()
			if len(fileName) > 9 && fileName[len(fileName)-9:] == ".data.csv" {
				spl := strings.Split(fileName, ".")
				if len(spl) == 4 {
					tbl, err := db.GetTable(spl[0], spl[1], conn)
					if err != nil {
						log.Fatal(err)
					}
					tables = append(tables, tbl)
				}
			}
		}
		db.SortTablesByForeignKey(tables)
		if _, ok := s["clear"]; ok {
			for i = len(tables) - 1; i >= 0; i-- {
				err = deleteTableData(tables[i], conn)
				if err != nil {
					log.Fatal(err)
				}

			}
		}
		for _, tbl := range tables {
			fn := tbl.Schema + "." + tbl.Name + ".data.csv"
			fmt.Print("Loading data from: ", fn, "... ")
			err = csv2table(fn, tbl, conn)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func deleteTableData(tbl db.Table, conn db.Conn) error {
	fmt.Print("Clearing ", tbl.Schema+"."+tbl.Name, "... ")
	query := "delete from " + conn.Quote(tbl.Schema+"."+tbl.Name)
	_, n, err := conn.Execute(query)
	fmt.Println(n, "records.")
	return err
}

//table2csv puts table data in csv file named schema.table.data.csv
func table2csv(tbl *db.Table, conn db.Conn) error {
	var filename, header, query, line string
	var value interface{}
	filename = tbl.Schema + "." + tbl.Name + ".data.csv"
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	query = "select "
	for i, c := range tbl.Columns {
		header += c.Name
		query += conn.Quote(c.Name)
		if i < len(tbl.Columns)-1 {
			header += ","
			query += ", "
		}
	}
	f.WriteString(header + "\n")
	f.Sync()
	query += " from " + conn.Quote(tbl.Schema+"."+tbl.Name)
	rows, err := conn.GetConnection().Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		rows.Scan(scanArgs...)
		line = ""
		for i, col := range values {
			if col == nil {
				value = ""
			} else {
				value = string(col)
			}
			value = strings.Replace(value.(string), "\"", "\"\"", -1)
			switch tbl.Columns[i].Type {
			case "string":
				line += "\"" + value.(string) + "\""
			case "dbdate":
				if value.(string) == "" {
					line += "null"
				} else {
					line += "\"" + value.(string) + "\""
				}
			case "int":
				if value.(string) == "" {
					value = "0"
				}
				line += value.(string)
			case "float":
				if value.(string) == "" {
					value = "0"
				}
				line += value.(string)
			case "bool":
				if value.(string) == "" {
					value = "0"
				}
				line += value.(string)
			default:
				line += value.(string)
			}
			if i < len(values)-1 {
				line += ","
			}
		}
		line += "\n"
		f.WriteString(line)
	}
	f.Sync()
	return nil
}

//csv2table loads data from csv file into table
func csv2table(fileName string, table db.Table, conn db.Conn) error {
	var spl, columns []string
	var query, values string
	var err error
	var counter int
	spl = strings.Split(fileName, ".")
	if len(spl) != 4 {
		return errors.New("Invalid file: " + fileName)
	}
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	csv := csv.NewReader(file)
	columns, err = csv.Read()
	if err != nil {
		return err
	}
	query = "insert into " + conn.Quote(spl[0]+"."+spl[1]) + " ("
	for i, s := range columns {
		query += conn.Quote(s)
		// query += s
		values += "?"
		if i < len(columns)-1 {
			query += ", "
			values += ","
		}
	}
	query += ") "
	tx, err := conn.GetConnection().Begin()
	if err != nil {
		return err
	}
	query = strings.Map(func(r rune) rune {
		if unicode.IsGraphic(r) {
			return r
		}
		return -1
	}, query)

	for {
		spline, err := csv.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		values = ""
		for i, v := range spline {
			if v == "null" {
				values += "null"
			} else {
				values += "'" + strings.Replace(v, "'", "''", -1) + "'"
			}
			if i < len(spline)-1 {
				values += ","
			}
		}
		if len(spline) != len(columns) {
			tx.Rollback()
			return errors.New("Inconsistent number of fields in file " + fileName + " " + strconv.Itoa(len(spline)) + "<>" + strconv.Itoa(len(columns)))
		}
		values := strings.Map(func(r rune) rune {
			if unicode.IsGraphic(r) {
				return r
			}
			return -1
		}, values)
		_, err = tx.Exec(query + " values (" + values + ");")
		if err != nil {
			tx.Rollback()
			return errors.New(err.Error() + " data: " + values)
		}
		counter++
	}
	fmt.Println(counter, "records.")
	return tx.Commit()
}
