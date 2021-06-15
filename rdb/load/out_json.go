/*
 *Descript:将object输出为json格式
 */
package load

import (
	"context"
	"io"

	"github.com/qianxiansheng90/go-redis-parser/rdb/parser"
)

type OutJson struct {
	writer io.Writer
}

// 输出为json
func ParseRDBOutJson(ctx context.Context, reader io.Reader, writer io.Writer, arg parser.ParseArg) (string, error) {
	var o = OutJson{
		writer: writer,
	}
	return ParseRDBHandler(ctx, reader, o.writeJSON, arg)
}

// out json format
func (o *OutJson) writeJSON(ctx context.Context, object parser.TypeObject) error {
	data, err := object.JSON()
	if err != nil {
		return err
	}
	if _, err = o.writer.Write(data); err != nil {
		return nil
	}
	_, err = o.writer.Write(returnByte)
	return err
}
