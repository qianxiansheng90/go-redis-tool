/*
*DESCRIPTION:解析select db
*/
package parser

import (
	"encoding/json"
	"fmt"
	"time"
)

type SelectionDB struct {
	Index uint64
}

func (p *RDBParser) selection(index uint64) error {
	return p.write(SelectionDB{Index: index})

}

func (s SelectionDB) Type() string {
	return ObjectTypeSelectDB
}

func (s SelectionDB) String() string {
	return fmt.Sprintf("{Select: %d}", s.Index)
}

func (s SelectionDB) Key() string {
	return "select"
}

func (s SelectionDB) Value() string {
	return ToString(s.Index)
}

func (s SelectionDB) ValueLen() uint64 {
	return 0
}

func (s SelectionDB) Command() (string, []interface{}, time.Time) {
	key := "select"
	var val = []interface{}{s.Index}
	exp := time.Time{}
	return key, val, exp
}
func (s SelectionDB) JSON() ([]byte, error) {
	return json.Marshal(s)
}
func (s SelectionDB) KV() ([]byte, error) {
	return []byte(fmt.Sprintf(OutInfoFormat, s.Key(), s.Value())), nil
}
func (s SelectionDB) ConcreteSize() uint64 {
	return 0
}
