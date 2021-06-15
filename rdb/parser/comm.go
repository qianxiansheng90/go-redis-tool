/*
 *Descript:rdb protocol(rdb.h)
 */
package parser

import "math"

// https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format
// Redis Object type
const (
	TypeString = iota
	TypeList
	TypeSet
	TypeZset
	TypeHash
	TypeZset2 /* ZSET version 2 with doubles stored in binary. */
	TypeModule
	TypeModule2 // module not support at present.
	_
	TypeHashZipMap
	TypeListZipList
	TypeSetIntSet
	TypeZsetZipList
	TypeHashZipList
	TypeListQuickList
	TypeStreamListPacks
)

/* Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType). */
const (
	FlagOpcodeModuleAux    = 247 /* Module auxiliary data. */
	FlagOpcodeIdle         = 248 /* LRU idle time. */
	FlagOpcodeFreq         = 249 /* LFU frequency. */
	FlagOpcodeAux          = 250 /* RDB aux field. */
	FlagOpcodeResizeDB     = 251 /* Hash table resize hint. */
	FlagOpcodeExpireTimeMs = 252 /* Expire time in milliseconds. */
	FlagOpcodeExpireTime   = 253 /* Old expire time in seconds. */
	FlagOpcodeSelectDB     = 254 /* DB number of the following keys. */
	FlagOpcodeEOF          = 255 /* End of the RDB file. */
)

// Redis length type
const (
	Type6Bit   = 0
	Type14Bit  = 1
	Type32Bit  = 0x80
	Type64Bit  = 0x81
	TypeEncVal = 3
)

/* Module serialized values sub opcodes */
const (
	TypeModuleOpcodeEof    = 0 /* End of module value. */
	TypeModuleOpcodeSInt   = 1 /* Signed integer. */
	TypeModuleOpcodeUInt   = 2 /* Unsigned integer. */
	TypeModuleOpcodeFloat  = 3 /* Float. */
	TypeModuleOpcodeDouble = 4 /* Double. */
	TypeModuleOpcodeString = 5 /* String. */
)

// ziplist type
const (
	ZipStr06B = 0
	ZipStr14B = 1
	ZipStr32B = 2

	ZipBigPrevLen = 0xfe
)

// ziplist entry
const (
	ZipInt04B = 15
	ZipInt08B = 0xfe        // 11111110
	ZipInt16B = 0xc0 | 0<<4 // 11000000
	ZipInt24B = 0xc0 | 3<<4 // 11110000
	ZipInt32B = 0xc0 | 1<<4 // 11010000
	ZipInt64B = 0xc0 | 2<<4 //11100000

)

// stream item
const (
	StreamItemFlagNone       = 0      /* No special flags. */
	StreamItemFlagDeleted    = 1 << 0 /* Entry was deleted. Skip it. */
	StreamItemFlagSameFields = 1 << 1 /* Same fields as master entry. */

)

// header
const (
	REDIS      = "REDIS"
	VersionMin = 1
	VersionMax = 9
)

// 对象类型
const (
	ObjectTypeAux       = "AuxField"
	ObjectTypeSelectDB  = "SelectDB"
	ObjectTypeResizeDB  = "ResizeDB"
	ObjectTypeKey       = "Key"
	ObjectTypeString    = "String"
	ObjectTypeHash      = "Hash"
	ObjectTypeSet       = "Set"
	ObjectTypeSortedSet = "SortedSet"
	ObjectTypeList      = "List"
	ObjectTypeStream    = "Stream"
)

var BasicObjectArray = []string{ObjectTypeString, ObjectTypeHash, ObjectTypeSet, ObjectTypeSortedSet, ObjectTypeList, ObjectTypeStream}

var (
	PosInf = math.Inf(1)
	NegInf = math.Inf(-1)
	Nan    = math.NaN()
)


