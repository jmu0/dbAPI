package mysql

import (
	"strings"

	"github.com/jmu0/dbAPI/db"
)

//save can be used by HandleREST and DbObject
// func save(dbName string, tblName string, cols []db.Column) (int, int, error) {
// 	var err error
// 	db, err := Connect(map[string]string{"database": "database", "username": "web", "password": "jmu0!"})
// 	if err != nil {
// 		return -1, -1, err
// 	}
// 	defer db.Close()

// 	query := "insert into " + dbName + "." + tblName + " "
// 	fields := "("
// 	strValues := "("
// 	insValues := make([]interface{}, 0)
// 	updValues := make([]interface{}, 0)
// 	strUpdate := ""
// 	for _, c := range cols {
// 		//log.Println("DEBUG:", c)
// 		if c.Value != nil {
// 			if (GetType(c.Type) == "int" && c.Value == "") == false { //skip auto_increment column
// 				if len(fields) > 1 {
// 					fields += ", "
// 				}
// 				fields += c.Field
// 				if len(strValues) > 1 {
// 					strValues += ", "
// 				}
// 				strValues += "?"
// 				insValues = append(insValues, c.Value)
// 				if len(strUpdate) > 0 {
// 					strUpdate += ", "
// 				}
// 				strUpdate += c.Field + "=?"
// 				updValues = append(updValues, c.Value)
// 			}
// 		}
// 	}
// 	fields += ")"
// 	strValues += ")"
// 	query += fields + " values " + strValues
// 	query += " on duplicate key update " + strUpdate
// 	// log.Println("DEBUG SAVE query:", query)
// 	insValues = append(insValues, updValues...)
// 	// log.Println("DEB:UG: query:", query, " insValues:", insValues)
// 	qr, err := db.Exec(query, insValues...)
// 	// log.Println("DEBUG:qr", qr, " err:", err)
// 	// stmt, err := db.Prepare(query)
// 	// if err != nil {
// 	// 	return -1, -1, err
// 	// }
// 	// qr, err := stmt.Exec(insValues...)
// 	if err != nil {
// 		return -1, -1, err
// 	}

// 	id, err := qr.LastInsertId()
// 	if err != nil {
// 		id = -1
// 	}
// 	n, err := qr.RowsAffected()
// 	if err != nil {
// 		n = -1
// 	}
// 	// fmt.Println("REST: DEBUG: save result n:", n, "id:", id)
// 	return int(n), int(id), nil
// }

// //delete can be used by HandleREST and DbObject
// func delete(dbName string, tblName string, cols []db.Column) (int, error) {
// 	db, err := Connect(map[string]string{"database": "database", "username": "web", "password": "jmu0!"})
// 	if err != nil {
// 		return 1, err
// 	}
// 	defer db.Close()
// 	query := "delete from " + dbName + "." + tblName + " where"
// 	where, err := StrPrimaryKeyWhereSQL(cols)
// 	if err != nil {
// 		fmt.Println("error:", err)
// 	}
// 	query += where
// 	res, err := db.Exec(query)
// 	if err != nil {
// 		return 1, err
// 	}
// 	nrrows, _ := res.RowsAffected()
// 	if nrrows < 1 {
// 		return 1, errors.New("No rows deleted")
// 	}
// 	return 0, nil
// }

func setAutoIncColumn(id int, cols []db.Column) []db.Column {
	//fmt.Println("DEBUG:setAutoIncColumn")
	for index, col := range cols {
		if strings.Contains(col.Type, "int") && col.Key == "PRI" {
			//fmt.Println("DEBUG:found", col.Field)
			cols[index].Value = id
		}
	}
	return cols
}
