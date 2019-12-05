package db

//DumpTable dumps table data to csv string
func DumpTable(table *Table, conn Conn) ([]byte, error) {
	//TODO: dump table data to sql
	return []byte(""), nil
}

//LoadData loads data from csv into database
func LoadData(schemaName, tableName string, bytes []byte, conn Conn) error {
	//TODO: load data from .csv file and insert in db
	// var lines, columns []string
	// var query string
	// var table Table
	// var err error
	// var i int
	// tbl, err = GetTable(schemaName, tableName, conn)
	// if err != nil {
	// 	return err
	// }
	// lines = strings.Split(string(bytes), "\n")
	// if len(lines) < 2 {
	// 	return errors.New("No lines")
	// }
	// query = "insert into "

	return nil
}
