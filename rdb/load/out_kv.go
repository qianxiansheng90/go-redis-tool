/*
 *Descript:将object输出为kv格式
 */
package load

import (
	"context"
	"io"

	"github.com/qianxiansheng90/go-redis-parser/rdb/parser"
)

type OutKV struct {
	writer io.Writer
}

var returnByte = []byte{0x0a}

// 输出为kv格式
func ParseRDBOutKV(ctx context.Context, reader io.Reader, writer io.Writer, arg parser.ParseArg) (string, error) {
	var o = OutKV{
		writer: writer,
	}
	return ParseRDBHandler(ctx, reader, o.writeKV, arg)
}

// out key value format
func (o *OutKV) writeKV(ctx context.Context, object parser.TypeObject) error {
	data, err := object.KV()
	if err != nil {
		return err
	}
	if _, err = o.writer.Write(data); err != nil {
		return nil
	}
	_, err = o.writer.Write(returnByte)
	return err
}
