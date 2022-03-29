/*
 *Descript:通用rdb handler处理器
 */
package load

import (
	"context"
	"io"

	"github.com/qianxiansheng90/go-redis-tool/rdb/parser"
)

// 创建一个解析器:outType  自定义输出
func ParseRDBHandler(ctx context.Context, reader io.Reader, f func(ctx context.Context, object parser.TypeObject) error, arg parser.ParseArg) (string, error) {
	p, err := parser.NewRDBParse(ctx, reader, f, nil, arg)
	if err != nil {
		return "", err
	}
	if err = p.Parse(); err != nil {
		return "", err
	}
	return p.GetRDBInfo(), p.Close()
}
