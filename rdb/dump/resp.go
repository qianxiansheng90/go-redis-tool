/*
 *Descript:resp协议
 */
package dump

import (
	"fmt"
	"net"
	"reflect"
	"strings"
)

type respType byte

const (
	typeString    respType = '+'
	typeError     respType = '-'
	typeInt       respType = ':'
	typeBulkBytes respType = '$'
	typeArray     respType = '*'
)

func (t respType) String() string {
	switch t {
	case typeString:
		return "<string>"
	case typeError:
		return "<error>"
	case typeInt:
		return "<int>"
	case typeBulkBytes:
		return "<bulkbytes>"
	case typeArray:
		return "<array>"
	default:
		if c := uint8(t); c > 0x20 && c < 0x7F {
			return fmt.Sprintf("<unknown-%c>", c)
		} else {
			return fmt.Sprintf("<unknown-0x%02x>", c)
		}
	}
}

type Resp interface {
}

type String struct {
	Value []byte
}

type Error struct {
	Value []byte
}

type Int struct {
	Value int64
}

func NewInt(n int64) *Int {
	return &Int{n}
}

type BulkBytes struct {
	Value []byte
}

func NewBulkBytes(p []byte) *BulkBytes {
	return &BulkBytes{p}
}

type Array struct {
	Value []Resp
}

func NewArray() *Array {
	return &Array{}
}

func (r *Array) Append(a Resp) {
	r.Value = append(r.Value, a)
}

func (r *Array) AppendBulkBytes(b []byte) {
	r.Append(NewBulkBytes(b))
}

func (r *Array) AppendInt(n int64) {
	r.Append(NewInt(n))
}

func AsString(r Resp, err error) ([]byte, error) {
	if err != nil {
		return make([]byte, 0), err
	}
	x, ok := r.(*String)
	if ok && x != nil {
		return x.Value, nil
	} else {
		return make([]byte, 0), fmt.Errorf("expect String, but got <%s>", reflect.TypeOf(r))
	}
}

func AsError(r Resp, err error) ([]byte, error) {
	if err != nil {
		return make([]byte, 0), err
	}
	x, ok := r.(*Error)
	if ok && x != nil {
		return x.Value, nil
	} else {
		return make([]byte, 0), fmt.Errorf("expect Error, but got <%s>", reflect.TypeOf(r))
	}
}

func AsBulkBytes(r Resp, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	x, ok := r.(*BulkBytes)
	if ok && x != nil {
		return x.Value, nil
	} else {
		return nil, fmt.Errorf("expect BulkBytes, but got <%s>", reflect.TypeOf(r))
	}
}

func AsInt(r Resp, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	x, ok := r.(*Int)
	if ok && x != nil {
		return x.Value, nil
	} else {
		return 0, fmt.Errorf("expect Int, but got <%s>", reflect.TypeOf(r))
	}
}

func AsArray(r Resp, err error) ([]Resp, error) {
	if err != nil {
		return nil, err
	}
	x, ok := r.(*Array)
	if ok && x != nil {
		return x.Value, nil
	} else {
		return nil, fmt.Errorf("expect Array, but got <%s>", reflect.TypeOf(r))
	}
}

func NewCommand(cmd string, args ...interface{}) Resp {
	r := NewArray()
	r.AppendBulkBytes([]byte(cmd))
	for i := 0; i < len(args); i++ {
		switch x := args[i].(type) {
		case nil:
			r.AppendBulkBytes(nil)
		case string:
			r.AppendBulkBytes([]byte(x))
		case []byte:
			r.AppendBulkBytes(x)
		default:
			r.AppendBulkBytes([]byte(fmt.Sprint(x)))
		}
	}
	return r
}

// read until hit the end of RESP: "\r\n"
func ReadRESPEnd(c net.Conn) (string, error) {
	var ret string
	for {
		b := make([]byte, 1)
		if _, err := c.Read(b); err != nil {
			return "", fmt.Errorf("read error[%v], current return[%s]", err, ret)
		}

		ret += string(b)
		if strings.HasSuffix(ret, "\r\n") {
			break
		}
	}
	return ret, nil
}

func RemoveRESPEnd(input string) string {
	length := len(input)
	if length >= 2 {
		return input[:length-2]
	}
	return input
}
