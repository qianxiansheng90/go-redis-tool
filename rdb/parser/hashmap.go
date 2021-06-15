/*
*DESCRIPTION:rdb 解析hash
*/
package parser

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Some of HashEntry manager.
type HashMap struct {
	Field  []byte      `json:"field"`
	Len    uint64      `json:"len"`
	Entry  []HashEntry `json:"entry"`
	Expire int64       `json:"expire"`
}

// HashTable entry.
type HashEntry struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

func (p *RDBParser) readHashMap(key KeyObject) error {
	length, _, err := p.loadLen()
	if err != nil {
		return err
	}
	hashTable := HashMap{
		Field:  key.Field,
		Len:    length,
		Entry:  make([]HashEntry, 0, length),
		Expire: key.Expire,
	}
	for i := uint64(0); i < length; i++ {
		field, err := p.loadString()
		if err != nil {
			return err
		}
		value, err := p.loadString()
		if err != nil {
			return err
		}
		hashTable.Entry = append(hashTable.Entry, HashEntry{Field: ToString(field), Value: ToString(value)})
	}
	return p.write(hashTable)
}

func (p *RDBParser) readHashMapWithZipmap(key KeyObject) error {
	zipmap, err := p.loadString()
	if err != nil {
		return err
	}
	buf := newInput(zipmap)
	blen, err := buf.ReadByte()
	if err != nil {
		return err
	}

	length := int(blen)
	if blen > 254 {
		length, err = countZipmapItems(buf)
		if err != nil {
			return err
		}
		length /= 2
	}

	hashTable := HashMap{
		Field:  key.Field,
		Len:    uint64(length),
		Entry:  make([]HashEntry, 0, length),
		Expire: key.Expire,
	}
	for i := 0; i < length; i++ {
		field, err := loadZipmapItem(buf, false)
		if err != nil {
			return err
		}
		value, err := loadZipmapItem(buf, true)
		if err != nil {
			return err
		}
		hashTable.Entry = append(hashTable.Entry, HashEntry{Field: ToString(field), Value: ToString(value)})
	}
	return p.write(hashTable)
}

func (p *RDBParser) readHashMapZiplist(key KeyObject) error {
	b, err := p.loadString()
	if err != nil {
		return err
	}
	buf := newInput(b)
	length, err := loadZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2

	hashTable := HashMap{
		Field:  key.Field,
		Len:    uint64(length),
		Entry:  make([]HashEntry, 0, length),
		Expire: key.Expire,
	}
	for i := int64(0); i < length; i++ {
		field, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		value, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		hashTable.Entry = append(hashTable.Entry, HashEntry{Field: ToString(field), Value: ToString(value)})
	}
	return p.write(hashTable)
}

func (hm HashMap) Type() string {
	return ObjectTypeHash
}

func (hm HashMap) String() string {
	return fmt.Sprintf("{HashMap: {Key: %s, Len: %d, Entries: %s}}", hm.Key(), hm.Len, hm.Value())
}

func (hm HashMap) Key() string {
	return ToString(hm.Field)
}

func (hm HashMap) Value() string {
	if len(hm.Entry) > 0 {
		itemStr, _ := json.Marshal(hm.Entry)
		return ToString(itemStr)
	}
	return ""
}

func (hm HashMap) ValueLen() uint64 {
	return uint64(len(hm.Entry))
}

func (hm HashMap) Command() (string, []interface{}, time.Time) {
	key := hm.Key()
	var val []interface{}
	for _, k := range hm.Entry {
		val = append(val, k.Field)
		val = append(val, k.Value)
	}
	exp := ToTime(hm.Expire)
	return key, val, exp
}

// 计算 hash 结构 field + value 的大小
func (hm HashMap) ConcreteSize() uint64 {
	kv := make([]string, 0, len(hm.Entry))
	for _, val := range hm.Entry {
		tmp := val
		kv = append(kv, tmp.Field+tmp.Value)
	}
	return uint64(len(strings.Join(kv, "")))
}

func (hm HashMap) JSON() ([]byte, error) {
	if hm.Expire > 0 {
		return json.Marshal(JSONExpireFormat{
			KeyType: ObjectTypeHash,
			Key:     hm.Key(),
			Value:   hm.Entry,
			Expire:  hm.Expire,
		})
	}
	return json.Marshal(JSONFormat{
		KeyType: ObjectTypeHash,
		Key:     hm.Key(),
		Value:   hm.Entry,
	})
}
func (hm HashMap) KV() ([]byte, error) {
	return []byte(fmt.Sprintf(OutKVFormat, ObjectTypeHash, hm.Key(), hm.Value(), hm.Expire)), nil
}
