/*
 *Descript:rdb的object输出
 */
package parser

const (
	beginning  = 1
	invalidExp = 0

	OutTypeJson = "json" // 输出json格式
	OutTypeKV   = "kv"   // 输出kv格式
	OutTypeLoad = "load" // 加载到redis
	OutTypeSelf = "self" // 输出到自定义writer

	ErrNotSupportOutType = "not support out type"
	ErrIllegalZSetData   = "illegal zset data"
	ErrUnknownDataFormat = "unknown data format"
	OutInfoFormat        = "info %s:%s"
)

// write out data
func (p *RDBParser) write(object TypeObject) error {
	switch object.Type() {
	case AuxField{}.Type():
		if p.parseArg.ExtInfo == false {
			return nil
		}
	default:
	}
	return p.handler(p.ctx, object)
}
