/*
 *Descript:rdb解析器
 */
package parser

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

// string encoding(rdb.h)
const (
	EncodeInt8  = iota /* 8 bit signed integer */
	EncodeInt16        /* 16 bit signed integer */
	EncodeInt32        /* 32 bit signed integer */
	EncodeLZF          /* string compressed with FASTLZ */
)

func (p *RDBParser) loadObject(key []byte, t byte, expire int64) error {
	keyObj := NewKeyObject(key, expire)
	var err error
	switch t {
	case TypeString:
		err = p.readString(keyObj)
	case TypeList:
		err = p.readList(keyObj)
	case TypeSet:
		err = p.readSet(keyObj)
	case TypeZset, TypeZset2:
		err = p.readZSet(keyObj, t)
	case TypeHash:
		keyObj := NewKeyObject(key, expire)
		err = p.readHashMap(keyObj)
	case TypeModule, TypeModule2:
		err = errors.New("not support module type! ")
	case TypeHashZipMap:
		err = p.readHashMapWithZipmap(keyObj)
	case TypeListZipList:
		err = p.readListWithZipList(keyObj)
	case TypeSetIntSet:
		err = p.readIntSet(keyObj)
	case TypeZsetZipList:
		err = p.readZipListSortSet(keyObj)
	case TypeHashZipList:
		err = p.readHashMapZiplist(keyObj)
	case TypeListQuickList: // quicklist + ziplist to realize linked list
		err = p.readListWithQuickList(keyObj)
	case TypeStreamListPacks:
		err = p.loadStreamListPack(keyObj)
	default:
		err = errors.New("not support key type:" + string(t))
	}
	return err
}

// get length
func (p *RDBParser) loadLen() (length uint64, isEncode bool, err error) {
	buf, err := p.reader.ReadByte()
	if err != nil {
		return
	}
	typeLen := (buf & 0xc0) >> 6
	if typeLen == TypeEncVal || typeLen == Type6Bit {
		/* Read a 6 bit encoding type or 6 bit len. */
		if typeLen == TypeEncVal {
			isEncode = true
		}
		length = uint64(buf) & 0x3f
	} else if typeLen == Type14Bit {
		/* Read a 14 bit len, need read next byte. */
		nb, err := p.reader.ReadByte()
		if err != nil {
			return 0, false, err
		}
		length = (uint64(buf)&0x3f)<<8 | uint64(nb)
	} else if buf == Type32Bit {
		_, err = io.ReadFull(p.reader, p.buff[0:4])
		if err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint32(p.buff))
	} else if buf == Type64Bit {
		_, err = io.ReadFull(p.reader, p.buff)
		if err != nil {
			return
		}
		length = binary.BigEndian.Uint64(p.buff)
	} else {
		err = errors.New(fmt.Sprintf("unknown length encoding %d in loadLen()", typeLen))
	}

	return
}

func (p *RDBParser) loadString() ([]byte, error) {
	length, needEncode, err := p.loadLen()
	if err != nil {
		return nil, err
	}

	if needEncode {
		switch length {
		case EncodeInt8:
			b, err := p.reader.ReadByte()
			return []byte(strconv.Itoa(int(b))), err
		case EncodeInt16:
			b, err := p.loadUint16()
			return []byte(strconv.Itoa(int(b))), err
		case EncodeInt32:
			b, err := p.loadUint32()
			return []byte(strconv.Itoa(int(b))), err
		case EncodeLZF:
			res, err := p.loadLZF()
			return res, err
		default:
			return []byte{}, errors.New("Unknown string encode type ")
		}
	}

	res := make([]byte, length)
	_, err = io.ReadFull(p.reader, res)
	return res, err
}

func (p *RDBParser) loadUint16() (res uint16, err error) {
	_, err = io.ReadFull(p.reader, p.buff[:2])
	if err != nil {
		return
	}

	res = binary.LittleEndian.Uint16(p.buff[:2])
	return
}

func (p *RDBParser) loadUint32() (res uint32, err error) {
	_, err = io.ReadFull(p.reader, p.buff[:4])
	if err != nil {
		return
	}
	res = binary.LittleEndian.Uint32(p.buff[:4])
	return
}

func (p *RDBParser) loadFloat() (float64, error) {
	b, err := p.reader.ReadByte()
	if err != nil {
		return 0, err
	}
	if b == 0xff {
		return NegInf, nil
	} else if b == 0xfe {
		return PosInf, nil
	} else if b == 0xfd {
		return Nan, nil
	}

	floatBytes := make([]byte, b)
	_, err = io.ReadFull(p.reader, floatBytes)
	if err != nil {
		return 0, err
	}
	float, err := strconv.ParseFloat(string(floatBytes), 64)
	return float, err
}

// 8 bytes float64, follow IEEE754 float64 stddef (standard definitions)
func (p *RDBParser) loadBinaryFloat() (float64, error) {
	if _, err := io.ReadFull(p.reader, p.buff); err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(p.buff)
	return math.Float64frombits(bits), nil
}

func (p *RDBParser) loadLZF() (res []byte, err error) {
	ilength, _, err := p.loadLen()
	if err != nil {
		return
	}
	ulength, _, err := p.loadLen()
	if err != nil {
		return
	}
	val := make([]byte, ilength)
	_, err = io.ReadFull(p.reader, val)
	if err != nil {
		return
	}
	res = lzfDecompress(val, int(ilength), int(ulength))
	return
}
