/*
DESCRIPTION:rdb 解析list
*/
package parser

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type ListObject struct {
	Field   []byte   `json:"field"`
	Len     uint64   `json:"len"`
	Entries []string `json:"entries"`
	Expire  int64    `json:"expire"`
}

func (p *RDBParser) readList(key KeyObject) error {
	length, _, err := p.loadLen()
	if err != nil {
		return err
	}
	listObj := ListObject{
		Field:   key.Field,
		Len:     length,
		Entries: make([]string, 0, length),
		Expire:  key.Expire,
	}
	for i := uint64(0); i < length; i++ {
		val, err := p.loadString()
		if err != nil {
			return err
		}
		listObj.Entries = append(listObj.Entries, ToString(val))
	}
	return p.write(listObj)
}

func (p *RDBParser) readListWithQuickList(key KeyObject) error {
	length, _, err := p.loadLen()
	if err != nil {
		return err
	}

	for i := uint64(0); i < length; i++ {
		listItems, err := p.loadZipList()
		if err != nil {
			return err
		}
		listObj := ListObject{
			Field:   key.Field,
			Len:     uint64(len(listItems)),
			Entries: make([]string, 0, len(listItems)),
			Expire:  key.Expire,
		}
		for _, v := range listItems {
			listObj.Entries = append(listObj.Entries, ToString(v))
		}
		if err = p.write(listObj); err != nil {
			return err
		}
	}

	return nil
}

func (p *RDBParser) readListWithZipList(key KeyObject) error {
	entries, err := p.loadZipList()
	if err != nil {
		return err
	}
	listObj := ListObject{
		Field:   key.Field,
		Len:     uint64(len(entries)),
		Entries: make([]string, 0, len(entries)),
		Expire:  key.Expire,
	}
	for _, v := range entries {
		listObj.Entries = append(listObj.Entries, ToString(v))
	}
	return p.write(listObj)
}

func (p *RDBParser) loadZipList() ([][]byte, error) {
	b, err := p.loadString()
	if err != nil {
		return nil, err
	}
	buf := newInput(b)
	length, err := loadZiplistLength(buf)
	if err != nil {
		return nil, err
	}

	items := make([][]byte, 0, length)
	for i := int64(0); i < length; i++ {
		entry, err := loadZiplistEntry(buf)
		if err != nil {
			return nil, err
		}
		items = append(items, entry)
	}

	return items, nil
}

func (l ListObject) Type() string {
	return ObjectTypeList
}

func (l ListObject) String() string {
	return fmt.Sprintf("{List: {Key: %s, Len: %d, Items: %s}}", l.Key(), l.Len, l.Value())
}

func (l ListObject) Key() string {
	return ToString(l.Field)
}

func (l ListObject) Value() string {
	return strings.Join(l.Entries, ",")
}

func (l ListObject) ValueLen() uint64 {
	return uint64(len(l.Entries))
}

func (l ListObject) Command() (string, []interface{}, time.Time) {
	key := ToString(l.Field)
	val := []interface{}{l.Entries}
	exp := ToTime(l.Expire)
	return key, val, exp
}

// list 结构计算所有item
func (l ListObject) ConcreteSize() uint64 {
	return uint64(len([]byte(l.Value())) - (len(l.Entries) - 1)) // 减去分隔符占用字节数
}

func (l ListObject) JSON() ([]byte, error) {
	if l.Expire > 0 {
		return json.Marshal(JSONExpireFormat{
			KeyType: ObjectTypeList,
			Key:     l.Key(),
			Value:   l.Entries,
			Expire:  l.Expire,
		})
	}
	return json.Marshal(JSONFormat{
		KeyType: ObjectTypeList,
		Key:     l.Key(),
		Value:   l.Entries,
	})
}
func (l ListObject) KV() ([]byte, error) {
	return []byte(fmt.Sprintf(OutKVFormat, ObjectTypeList, l.Key(), l.Value(), l.Expire)), nil
}
