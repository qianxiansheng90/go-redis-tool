/*
 * @Author: 基础架构部.钱芳园
 * @Date: 2021-05-06 14:17:09
 * @Last Modified by: 基础架构部.钱芳园
 * @Last Modified time: 2021-05-06 14:18:48
 */
/*
 *Descript:rdb key obj
 */
package parser

import (
	"fmt"
	"time"
)

type KeyObject struct {
	Field  []byte
	Expire int64
}

func NewKeyObject(key []byte, expire int64) KeyObject {
	return KeyObject{Field: key, Expire: expire}
}

// Whether the key has expired until now.
func (k KeyObject) GetExpireTime() time.Time {
	return time.Unix(k.Expire/1000, 0).UTC()
}

// Whether the key has expired until now.
func (k KeyObject) Expired() bool {
	return k.GetExpireTime().Before(time.Now())
}

func (k KeyObject) Type() string {
	return ObjectTypeKey
}

func (k KeyObject) String() string {
	if k.Expire > 0 {
		return fmt.Sprintf("{ExpiryTime: %s, Key: %s}", k.GetExpireTime(), k.Value())
	}

	return fmt.Sprintf("%s", k.Value())
}

func (k KeyObject) Key() string {
	return ""
}

func (k KeyObject) Value() string {
	return ToString(k.Field)
}

func (k KeyObject) ValueLen() uint64 {
	return k.ConcreteSize()
}

// 暂时返回key的长度
func (k KeyObject) ConcreteSize() uint64 {
	return uint64(len([]byte(k.Value())))
}
