/*
 *Descript:解析string
 */
package parser

import (
	"encoding/json"
	"fmt"
	"time"
)

type StringObject struct {
	Field  []byte `json:"field"`
	Val    []byte `json:"val"`
	Expire int64  `json:"expire"`
}

func (p *RDBParser) readString(key KeyObject) error {
	valBytes, err := p.loadString()
	if err != nil {
		return err
	}
	return p.write(NewStringObject(key, valBytes))
}

func NewStringObject(key KeyObject, val []byte) StringObject {
	return StringObject{
		Field:  key.Field,
		Val:    val,
		Expire: key.Expire,
	}
}

func (s StringObject) String() string {
	return fmt.Sprintf("{String: {Key: %s, Value:'%s'}}", s.Key(), s.Value())
}

func (s StringObject) Type() string {
	return ObjectTypeString
}

func (s StringObject) Key() string {
	return ToString(s.Field)
}

func (s StringObject) Value() string {
	return ToString(s.Val)
}

func (s StringObject) ValueLen() uint64 {
	return s.ConcreteSize()
}

func (s StringObject) Command() (string, []interface{}, time.Time) {
	key := ToString(s.Field)
	val := []interface{}{s.Value()}
	return key, val, ToTime(s.Expire)
}

// String类型，计算对应value
func (s StringObject) ConcreteSize() uint64 {
	return uint64(len([]byte(s.Value())))
}
func (s StringObject) JSON() ([]byte, error) {
	if s.Expire > 0 {
		return json.Marshal(JSONExpireFormat{
			KeyType: ObjectTypeString,
			Key:     s.Key(),
			Value:   s.Value(),
			Expire:  s.Expire,
		})
	}
	return json.Marshal(JSONFormat{
		KeyType: ObjectTypeString,
		Key:     s.Key(),
		Value:   s.Value(),
	})
}
func (s StringObject) KV() ([]byte, error) {
	return []byte(fmt.Sprintf(OutKVFormat, ObjectTypeString, s.Key(), s.Value(), s.Expire)), nil
}
