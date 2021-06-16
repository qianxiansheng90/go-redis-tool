/*
*DESCRIPTION:解析set
*/
package parser

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Set struct {
	Field   []byte   `json:"field"`
	Len     uint64   `json:"len"`
	Entries []string `json:"entries"`
	Expire  int64    `json:"expire"`
}

func (p *RDBParser) readSet(key KeyObject) error {
	length, _, err := p.loadLen()
	if err != nil {
		return err
	}
	set := Set{
		Field:   key.Field,
		Len:     length,
		Entries: make([]string, 0, length),
		Expire:  key.Expire,
	}
	for i := uint64(0); i < length; i++ {
		member, err := p.loadString()
		if err != nil {
			return err
		}
		set.Entries = append(set.Entries, ToString(member))
	}

	return p.write(set)
}

func (p *RDBParser) readIntSet(key KeyObject) error {
	b, err := p.loadString()
	if err != nil {
		return err
	}
	buf := newInput(b)
	sizeBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	intSize := binary.LittleEndian.Uint32(sizeBytes)
	if intSize != 2 && intSize != 4 && intSize != 8 {
		return errors.New(fmt.Sprintf("unknown intset encoding: %d", intSize))
	}
	lenBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	cardinality := binary.LittleEndian.Uint32(lenBytes)
	//intSetItem := make([][]byte, 0, cardinality)
	set := Set{
		Field:   key.Field,
		Len:     uint64(cardinality),
		Entries: make([]string, 0, cardinality),
		Expire:  key.Expire,
	}
	for i := uint32(0); i < cardinality; i++ {
		intBytes, err := buf.Slice(int(intSize))
		if err != nil {
			return err
		}
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		set.Entries = append(set.Entries, ToString(intString))
	}
	return p.write(set)
}

func (s Set) Type() string {
	return ObjectTypeSet
}

func (s Set) String() string {
	return fmt.Sprintf("{Set: {Key: %s, Len: %d, Item: %s}}", s.Key(), s.Len, s.Value())
}

func (s Set) Key() string {
	return ToString(s.Field)
}

func (s Set) Value() string {
	return ToString(strings.Join(s.Entries, ","))
}

func (s Set) ValueLen() uint64 {
	return uint64(len(s.Entries))
}

func (s Set) Command() (string, []interface{}, time.Time) {
	key := ToString(s.Field)
	val := []interface{}{s.Entries}
	exp := ToTime(s.Expire)
	return key, val, exp
}

// Set 结构计算所有item
func (s Set) ConcreteSize() uint64 {
	return uint64(len([]byte(s.Value())) - (len(s.Entries) - 1))
}

func (s Set) JSON() ([]byte, error) {
	if s.Expire > 0 {
		return json.Marshal(JSONExpireFormat{
			KeyType: ObjectTypeSet,
			Key:     s.Key(),
			Value:   s.Entries,
			Expire:  s.Expire,
		})
	}
	return json.Marshal(JSONFormat{
		KeyType: ObjectTypeSet,
		Key:     s.Key(),
		Value:   s.Entries,
	})
}
func (s Set) KV() ([]byte, error) {
	return []byte(fmt.Sprintf(OutKVFormat, ObjectTypeSet, s.Key(), s.Value(), s.Expire)), nil
}
