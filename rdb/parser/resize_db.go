/*
* DESCRIPTION:解析resize db
*/
package parser

import (
	"encoding/json"
	"fmt"
	"time"
)

type ResizeDB struct {
	DBSize     uint64
	ExpireSize uint64
}

func (p *RDBParser) resize(dbSize, expireSize uint64) error {
	return p.write(ResizeDB{DBSize: dbSize, ExpireSize: expireSize})
}

func (r ResizeDB) String() string {
	return fmt.Sprintf("{ResizeDB: %s}", r.Value())
}

func (r ResizeDB) Key() string {
	return "resize db"
}

func (r ResizeDB) Value() string {
	return fmt.Sprintf("{DBSize: %d, ExpireSize: %d}", r.DBSize, r.ExpireSize)
}

func (r ResizeDB) ValueLen() uint64 {
	return 0
}

func (r ResizeDB) ConcreteSize() uint64 {
	return 0
}

func (r ResizeDB) Command() (string, []interface{}, time.Time) {
	key := "resize db"
	var val = []interface{}{r.Value()}
	exp := time.Time{}
	return key, val, exp
}
func (r ResizeDB) JSON() ([]byte, error) {
	return json.Marshal(r)
}
func (r ResizeDB) KV() ([]byte, error) {
	return []byte(fmt.Sprintf(OutInfoFormat, r.Key(), r.Value())), nil
}
func (r ResizeDB) Type() string {
	return ObjectTypeResizeDB
}
