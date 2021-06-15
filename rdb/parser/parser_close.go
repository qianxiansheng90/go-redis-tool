/*
 *Descript:rdb解析器
 */
package parser

const (
	ErrContextDone = "context done"
)

// 关闭解析器
func (p *RDBParser) Close() error {
	if p.closer != nil {
		return p.closer(p.ctx)
	}
	return nil
}
