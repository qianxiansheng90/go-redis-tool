package dump

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"strconv"
)

const (
	iMapSize = 1024*512 + 1024
)

type encoder struct {
	w *bufio.Writer
}

func itos(i int64) string {

	if n := i + 1024; n >= 0 && n < iMapSize {
		return strconv.FormatInt(n-1024, 10)
	} else {
		return strconv.FormatInt(i, 10)
	}
}

func Encode(w *bufio.Writer, r Resp, flush bool) error {
	e := &encoder{w}
	if err := e.encodeResp(r); err != nil {
		return err
	}
	if !flush {
		return nil
	}
	return w.Flush()
}

func MustEncode(w *bufio.Writer, r Resp) {
	if err := Encode(w, r, true); err != nil {
		panic(fmt.Sprintf("%s %s", err, "encode redis resp failed"))
	}
}

func EncodeToBytes(r Resp) ([]byte, error) {
	var b bytes.Buffer
	err := Encode(bufio.NewWriter(&b), r, true)
	return b.Bytes(), err
}

func EncodeToString(r Resp) (string, error) {
	var b bytes.Buffer
	err := Encode(bufio.NewWriter(&b), r, true)
	return b.String(), err
}

func MustEncodeToBytes(r Resp) []byte {
	b, err := EncodeToBytes(r)
	if err != nil {
		panic(err)
	}
	return b
}

func (e *encoder) encodeResp(r Resp) error {
	switch x := r.(type) {
	default:
		return fmt.Errorf("encode bad resp type <%s>", reflect.TypeOf(r))
	case *String:
		if err := e.encodeType(typeString); err != nil {
			return err
		}
		return e.encodeText(x.Value)
	case *Error:
		if err := e.encodeType(typeError); err != nil {
			return err
		}
		return e.encodeText(x.Value)
	case *Int:
		if err := e.encodeType(typeInt); err != nil {
			return err
		}
		return e.encodeInt(x.Value)
	case *BulkBytes:
		if err := e.encodeType(typeBulkBytes); err != nil {
			return err
		}
		return e.encodeBulkBytes(x.Value)
	case *Array:
		if err := e.encodeType(typeArray); err != nil {
			return err
		}
		return e.encodeArray(x.Value)
	}
}

func (e *encoder) encodeType(t respType) error {
	return e.w.WriteByte(byte(t))
}

func (e *encoder) encodeString(s string) error {
	if _, err := e.w.WriteString(s); err != nil {
		return err
	}
	if _, err := e.w.WriteString("\r\n"); err != nil {
		return err
	}
	return nil
}

func (e *encoder) encodeText(s []byte) error {
	if _, err := e.w.Write(s); err != nil {
		return err
	}
	if _, err := e.w.WriteString("\r\n"); err != nil {
		return err
	}
	return nil
}

func (e *encoder) encodeInt(v int64) error {
	return e.encodeString(itos(v))
}

func (e *encoder) encodeBulkBytes(b []byte) error {
	if b == nil {
		return e.encodeInt(-1)
	} else {
		if err := e.encodeInt(int64(len(b))); err != nil {
			return err
		}
		if _, err := e.w.Write(b); err != nil {
			return err
		}
		if _, err := e.w.WriteString("\r\n"); err != nil {
			return err
		}
		return nil
	}
}

func (e *encoder) encodeArray(a []Resp) error {
	if a == nil {
		return e.encodeInt(-1)
	} else {
		if err := e.encodeInt(int64(len(a))); err != nil {
			return err
		}
		for i := 0; i < len(a); i++ {
			if err := e.encodeResp(a[i]); err != nil {
				return err
			}
		}
		return nil
	}
}
