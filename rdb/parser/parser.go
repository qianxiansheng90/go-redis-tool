/*
 *Descript:rdb解析器
 */
package parser

import (
	"bufio"
	"context"
	"io"

	"github.com/pkg/errors"
)

const (
	NotExpired = -1
)

// 解析器结构体
type RDBParser struct {
	outType    string
	reader     *bufio.Reader                                      // 输入流
	parseArg   ParseArg                                           // 解析参数
	buff       []byte                                             // 缓冲区
	ctx        context.Context                                    // ctx
	handler    func(ctx context.Context, object TypeObject) error // 自定义处理器
	closer     func(ctx context.Context) error                    // 关闭
	rdbVersion string                                             // rdb版本
}

// 解析参数结构体
type ParseArg struct {
	ExtInfo bool // 输出一些额外的信息:例如版本
}

// 创建一个解析器:outType  输出类型:json,kv
func NewRDBParse(ctx context.Context, reader io.Reader, f func(ctx context.Context, object TypeObject) error,
	c func(ctx context.Context) error, arg ParseArg) (*RDBParser, error) {
	p := RDBParser{
		reader:   bufio.NewReader(reader),
		handler:  f,
		parseArg: arg,
		closer:   c,
		buff:     make([]byte, 8),
		ctx:      ctx,
	}

	if err := p.checkParser(); err != nil {
		return &p, err
	}
	return &p, nil
}

// 检查
func (p *RDBParser) checkParser() error {
	if p.handler == nil {
		return errors.New("handler is null")
	}
	if p.reader == nil {
		return errors.New("reader is null")
	}
	if p.ctx == nil {
		p.ctx = context.Background()
	}
	return nil
}

// 获取rdb版本
func (p *RDBParser) GetRDBInfo() string {
	return p.rdbVersion
}
