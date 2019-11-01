package mysql

import (
	"bytes"
	"compress/gzip"
)

func compress(inp []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(inp)
	if err != nil {
		return inp, err
	}
	if err := zw.Close(); err != nil {
		return inp, err
	}
	return []byte(buf.String()), nil
}
