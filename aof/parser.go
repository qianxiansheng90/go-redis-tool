/*
 *Descript:aof解析器
 */
package aof

import (
	"bufio"
	"io"
	"os"

	"github.com/pkg/errors"
)

// aof解析器
type AofParser struct {
	offset        int64
	nextCmdOffset int64
	file          *os.File
	reader        *bufio.Reader
}

// 解析文件
func NewAofFileParser(aofFilePath string) (*AofParser, error) {
	file, err := os.Open(aofFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "open file "+aofFilePath)
	}
	return &AofParser{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

// 解析reader流
func NewAofParser(reader io.Reader) (*AofParser, error) {
	return &AofParser{
		reader: bufio.NewReader(reader),
	}, nil
}
