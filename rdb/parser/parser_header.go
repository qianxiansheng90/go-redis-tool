/*
 *Descript:rdb解析器
 */
package parser

import (
	"bytes"
	"io"
	"strconv"

	"github.com/pkg/errors"
)

const (
	ErrRDBFileEmpty         = "RDB file is empty"
	ErrReadRDBFile          = "Read RDB file failed"
	ErrNotSupportRDBVersion = "not support rdb version"
)

// 9 bytes length include: 5 bytes "REDIS" and 4 bytes version in rdb.h
func (p *RDBParser) parseHeader() error {
	header := make([]byte, 9)
	_, err := io.ReadFull(p.reader, header)
	if err != nil {
		if err == io.EOF {
			return errors.New("RDB file is empty")
		}
		return errors.Wrap(err, ErrReadRDBFile)
	}

	// Check "REDIS" string and version.
	rdbVersion, err := strconv.Atoi(string(header[5:]))
	if !bytes.Equal(header[0:5], []byte(REDIS)) || err != nil || (rdbVersion < VersionMin || rdbVersion > VersionMax) {
		return errors.New(ErrNotSupportRDBVersion)
	}
	p.rdbVersion = string(header)
	return nil
}
