/*
 *Descript:AuxField字段解析
 */
package parser

import (
	"encoding/json"
	"fmt"
	"time"
)

type AuxField struct {
	Field string
	Val   string
}

func (p *RDBParser) auxFields(key, val []byte) error {
	return p.write(AuxField{Field: string(key), Val: string(val)})
}

func (af AuxField) String() string {
	return fmt.Sprintf("{Aux: {Key: %s, Value: %s}}", ToString(af.Field), ToString(af.Val))
}

func (af AuxField) Key() string {
	return ToString(af.Field)
}

func (af AuxField) Value() string {
	return ToString(af.Val)
}

func (af AuxField) ValueLen() uint64 {
	return 0
}

func (af AuxField) Type() string {
	return ObjectTypeAux
}

func (af AuxField) Command() (string, []interface{}, time.Time) {
	key := ToString(af.Field)
	val := []interface{}{ToString(af.Val)}
	exp := time.Time{}
	return key, val, exp
}

func (af AuxField) JSON() ([]byte, error) {
	return json.Marshal(JSONFormat{
		KeyType: ObjectTypeAux,
		Key:     af.Key(),
		Value:   af.Value(),
	})

}
func (af AuxField) KV() ([]byte, error) {
	return []byte(fmt.Sprintf(OutInfoFormat, af.Key(), af.Value())), nil
}

// 辅助字段全部返回0
func (af AuxField) ConcreteSize() uint64 {
	return 0
}
