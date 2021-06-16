/*
 *Descript:解析zset
 */
package parser

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type SortedSet struct {
	Field   []byte           `json:"field"`
	Len     uint64           `json:"len"`
	Entries []SortedSetEntry `json:"entries"`
	Expire  int64            `json:"expire"`
}

type SortedSetEntry struct {
	Field interface{} `json:"field"`
	Score float64     `json:"score"`
}

func (p *RDBParser) readZSet(key KeyObject, t byte) error {
	length, _, err := p.loadLen()
	if err != nil {
		return err
	}
	sortedSet := SortedSet{
		Field:   key.Field,
		Len:     length,
		Entries: make([]SortedSetEntry, 0, length),
		Expire:  key.Expire,
	}
	for i := uint64(0); i < length; i++ {
		member, err := p.loadString()
		if err != nil {
			return err
		}
		var score float64
		if t == TypeZset2 {
			score, err = p.loadBinaryFloat()
		} else {
			score, err = p.loadFloat()
		}
		if err != nil {
			return err
		}
		sortedSet.Entries = append(sortedSet.Entries, SortedSetEntry{Field: ToString(member), Score: score})
	}
	return p.write(sortedSet)
}

func (p *RDBParser) readZipListSortSet(key KeyObject) error {
	b, err := p.loadString()
	if err != nil {
		return err
	}
	buf := newInput(b)
	cardinality, err := loadZiplistLength(buf)
	if err != nil {
		return err
	}
	cardinality /= 2

	sortedSet := SortedSet{
		Field:   key.Field,
		Len:     uint64(cardinality),
		Entries: make([]SortedSetEntry, 0, cardinality),
		Expire:  key.Expire,
	}
	for i := int64(0); i < cardinality; i++ {
		member, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		scoreBytes, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		score, err := strconv.ParseFloat(string(scoreBytes), 64)
		if err != nil {
			return err
		}
		sortedSet.Entries = append(sortedSet.Entries, SortedSetEntry{Field: ToString(member), Score: score})
	}
	return p.write(sortedSet)
}

func (zs SortedSet) Type() string {
	return ObjectTypeSortedSet
}

func (zs SortedSet) String() string {
	return fmt.Sprintf("{SortedSet: {Key: %s, Len: %d, Entries: %s}}", zs.Key(), zs.Len, zs.Value())
}

func (zs SortedSet) Key() string {
	return ToString(zs.Field)
}

func (zs SortedSet) Value() string {
	itemStr, _ := json.Marshal(zs.Entries)
	return ToString(itemStr)
}

func (zs SortedSet) ValueLen() uint64 {
	return uint64(len(zs.Entries))
}

func (zs SortedSet) Command() (string, []interface{}, time.Time) {
	key := zs.Key()
	var val []interface{}
	for _, k := range zs.Entries {
		val = append(val, k)
	}
	exp := ToTime(zs.Expire)
	return key, val, exp
}
func (zs SortedSet) ConcreteSize() uint64 {
	var size uint64
	if len(zs.Entries) > 0 {
		for _, val := range zs.Entries {
			size += uint64(len([]byte(ToString(val.Field))))
		}
	}
	return size
}

func (zs SortedSet) JSON() ([]byte, error) {
	if zs.Expire > 0 {
		return json.Marshal(JSONExpireFormat{
			KeyType: ObjectTypeSortedSet,
			Key:     zs.Key(),
			Value:   zs.Entries,
			Expire:  zs.Expire,
		})
	}
	return json.Marshal(JSONFormat{
		KeyType: ObjectTypeSortedSet,
		Key:     zs.Key(),
		Value:   zs.Entries,
	})
}
func (zs SortedSet) KV() ([]byte, error) {
	return []byte(fmt.Sprintf(OutKVFormat, ObjectTypeSortedSet, zs.Key(), zs.Value(), zs.Expire)), nil
}
