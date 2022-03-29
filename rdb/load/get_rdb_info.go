/*
 *Descript:获取rdb信息
 */
package load

import (
	"context"
	"io"
	"os"
	"strconv"

	"github.com/pkg/errors"
	"github.com/qianxiansheng90/go-redis-tool/rdb/parser"
)

const (
	bigKeyValueSize = 1024 * 1024
	ErrCloseProcess = "close process"
)

// rdb信息输出
type RDBInfo struct {
	RDBVersion      string                              `json:"rdb_version"`
	RedisVersion    string                              `json:"redis_version"`
	RedisBits       int                                 `json:"redis_bit"`
	RedisCTime      int64                               `json:"c_time"`
	RedisUsedMemory int64                               `json:"used_memory"`
	KeyStatistics   map[int]map[string]BigKeyStatistics `json:"key_statistics"`
}

// 大key统计
type BigKeyStatistics struct {
	KeyTotalCount int64     `json:"big_key_count"`
	KeyTotalSize  uint64    `json:"big_key_total_size"`
	KeyList       []KeyInfo `json:"big_key_list"`
}

type KeyInfo struct {
	KeyName           string `json:"key_name"`
	KeyType           string `json:"key_type"`
	ValueTotalSize    uint64 `json:"value_total_size"`
	ValueTotalItemLen uint64 `json:"value_total_item_len"`
}

// 参数
type GetRDBInfoArg struct {
	OnlyRDBInfo   bool      `json:"only_rdb_info"`  // 只统计rdb信息
	KeyStatistics bool      `json:"key_statistics"` // 统计key信息
	BigKey        bool      `json:"big_key"`        // 大key输出
	BigKeyArg     BigKeyArg `json:"big_key_arg"`    // 大key的输出条件
}

// 参数:大key定义
type BigKeyArg struct {
	ValueSize uint64            `json:"value_size"` // 默认为1024字节
	TypeVal   map[string]BigKey `json:"type_value"` // key类型:{}
}

// 结构体
type rdbInfo struct {
	reader        io.ReadCloser // 输入流
	info          *RDBInfo      // 返回结果
	OnlyRDBInfo   bool          // 只统计rdb信息
	KeyStatistics bool          // 统计key信息
	BigKey        bool          // 大key输出
	ValueSize     uint64        // 默认为1024字节
	bigKey        map[string]BigKey
}

// 大key定义
type BigKey struct {
	ValueSize uint64 `json:"value_size"`
	MemberLen uint64 `json:"member_len"`
}

// 从文件中获取rdb信息
func GetRDBFileInfo(ctx context.Context, rdbFilePath string, arg GetRDBInfoArg) (*RDBInfo, error) {
	file, err := os.Open(rdbFilePath)
	if err != nil {
		return nil, errors.Wrap(err, rdbFilePath)
	}
	defer file.Close()
	return GetRDBInfo(ctx, file, arg)
}

// 从输出流中获取文件信息
func GetRDBInfo(ctx context.Context, reader io.ReadCloser, arg GetRDBInfoArg) (*RDBInfo, error) {
	var r = rdbInfo{
		reader:        reader,
		info:          &RDBInfo{},
		OnlyRDBInfo:   arg.OnlyRDBInfo,
		KeyStatistics: arg.KeyStatistics,
		BigKey:        arg.BigKey,
		ValueSize:     arg.BigKeyArg.ValueSize,
		bigKey:        map[string]BigKey{},
	}
	var err error
	r.init(arg)

	r.info.RDBVersion, err = ParseRDBHandler(ctx, reader, r.handler, parser.ParseArg{
		ExtInfo: true,
	})
	if err != nil {
		if err.Error() == ErrCloseProcess {
			return r.info, nil
		}
		return r.info, err
	}
	return r.info, nil
}

// 初始化
func (r *rdbInfo) init(arg GetRDBInfoArg) {
	if r.ValueSize <= 0 {
		r.ValueSize = bigKeyValueSize
	}
	r.info.KeyStatistics = make(map[int]map[string]BigKeyStatistics)
	var bigkey = make(map[string]BigKey)
	if arg.BigKeyArg.TypeVal == nil {
		arg.BigKeyArg.TypeVal = make(map[string]BigKey)
	}
	for _, kType := range parser.BasicObjectArray {
		val, exist := arg.BigKeyArg.TypeVal[kType]
		if exist == false {
			bigkey[kType] = BigKey{
				ValueSize: r.ValueSize,
				MemberLen: 0,
			}
			continue
		}
		if val.ValueSize == 0 {
			val.ValueSize = r.ValueSize
		}
		bigkey[kType] = BigKey{
			ValueSize: val.ValueSize,
			MemberLen: val.MemberLen,
		}
	}
	r.bigKey = bigkey
}

// 获取元素在数组中的索引
func arrayIndex(array []string, item string) int {
	for idx, k := range array {
		if k == item {
			return idx
		}
	}
	return 0
}

// 处理key
func (r *rdbInfo) handler(ctx context.Context, object parser.TypeObject) error {
	var dbNumber int = 0
	switch object.Type() {
	case parser.StringObject{}.Type(), parser.ListObject{}.Type(), parser.HashMap{}.Type(), parser.RedisStream{}.Type(), parser.Set{}.Type(),
		parser.SortedSet{}.Type():
		if r.BigKey == true {
			r.checkBigKey(dbNumber, object.Type(), object.Key(), object.ValueLen(), object.ConcreteSize())
			return nil
		}
		r.getKeySize(dbNumber, object.Type(), object.ConcreteSize())
	case parser.SelectionDB{}.Type():
		_, val, _ := object.Command()
		dbNum, ok := val[0].(uint64)
		if ok == false {
			return errors.New("internal error dbsize value")
		}
		dbNumber = int(dbNum)

	case parser.AuxField{}.Type():
		return r.getDBInfo(object)
	default:
	}
	return nil
}

// 获取db大小
func (r *rdbInfo) getDBInfo(object parser.TypeObject) error {
	var err error
	switch object.Key() {
	case "redis-ver":
		r.info.RedisVersion = object.Value()
	case "redis-bits":
		if r.info.RedisBits, err = strconv.Atoi(object.Value()); err != nil {
			return err
		}
	case "ctime":
		if r.info.RedisCTime, err = strconv.ParseInt(object.Value(), 10, 64); err != nil {
			return err
		}
	case "used-mem":
		if r.info.RedisUsedMemory, err = strconv.ParseInt(object.Value(), 10, 64); err != nil {
			return err
		}
	}
	if r.OnlyRDBInfo == true && r.info.RedisVersion != "" && r.info.RedisBits != 0 &&
		r.info.RedisCTime != 0 && r.info.RedisUsedMemory != 0 {
		return errors.New(ErrCloseProcess)
	}
	return nil
}

// 获取key统计
func (r *rdbInfo) getKeySize(dbNumber int, keyType string, valSize uint64) {
	if r.KeyStatistics == false {
		return
	}
	mapKeyStatistics, exist := r.info.KeyStatistics[dbNumber]
	if exist == false {
		mapKeyStatistics = map[string]BigKeyStatistics{}
	}
	keyStatistics, exist := mapKeyStatistics[keyType]
	if exist == false {
		keyStatistics = BigKeyStatistics{}
	}
	keyStatistics.KeyTotalCount++
	keyStatistics.KeyTotalSize += valSize
	mapKeyStatistics[keyType] = keyStatistics
	r.info.KeyStatistics[dbNumber] = mapKeyStatistics
}

// 检查大key
func (r *rdbInfo) checkBigKey(dbNumber int, keyType, keyName string, valLen, valSize uint64) {
	if r.BigKey == false {
		return
	}
	mapKeyStatistics, exist := r.info.KeyStatistics[dbNumber]
	if exist == false {
		mapKeyStatistics = map[string]BigKeyStatistics{}
	}
	keyStatistics, exist := mapKeyStatistics[keyType]
	if exist == false {
		keyStatistics = BigKeyStatistics{}
	}
	big, exist := r.bigKey[keyType]
	if exist == true && (big.ValueSize > 0 && valSize > big.ValueSize) || (big.MemberLen > 0 && valLen >= big.MemberLen) {
		keyStatistics.KeyTotalCount++
		keyStatistics.KeyTotalSize += valSize
		keyStatistics.KeyList = append(keyStatistics.KeyList, KeyInfo{
			KeyName:           keyName,
			KeyType:           keyType,
			ValueTotalSize:    valSize,
			ValueTotalItemLen: valLen,
		})
		mapKeyStatistics[keyType] = keyStatistics
		r.info.KeyStatistics[dbNumber] = mapKeyStatistics
	}
}
