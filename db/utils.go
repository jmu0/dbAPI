package db

import (
	"log"
	"strconv"
	"strings"
)

//Escape string to prevent common sql injection attacks
func Escape(str string) string {
	// ", ', 0=0
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "''", "'", -1)
	str = strings.Replace(str, "'", "''", -1)

	// \x00, \n, \r, \ and \x1a"
	str = strings.Replace(str, "\x00", "", -1)
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\r", "", -1)
	str = strings.Replace(str, "\x1a", "", -1)

	//multiline attack
	str = strings.Replace(str, ";", " ", -1)

	//comments attack
	str = strings.Replace(str, "--", "", -1)
	str = strings.Replace(str, "#", "", -1)
	str = strings.Replace(str, "/*", "", -1)
	str = strings.Replace(str, "*/", "", -1)
	return str
}

//Interface2string returns escaped string for interface{}
func Interface2string(val interface{}, quote bool) string {
	var value string
	if val == nil {
		return ""
	}
	switch t := val.(type) {
	case string:
		if quote {
			value += "'" + Escape(val.(string)) + "'"

		} else {
			value += Escape(val.(string))
		}
	case int, int32, int64:
		value += strconv.Itoa(val.(int))
	case []uint8:
		if quote {
			value += "'" + string([]byte(val.([]uint8))) + "'"
		} else {
			value += string([]byte(val.([]uint8)))
		}
	default:
		log.Println("WARNING: type not handled:", t, "using string")
		if quote {
			value += "'" + Escape(val.(string)) + "'"
		} else {
			value += Escape(val.(string))
		}
	}
	return value
}
