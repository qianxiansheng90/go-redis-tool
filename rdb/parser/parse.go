/*
 *Descript:rdb解析器
 */
package parser

import (
	"encoding/binary"
	"errors"
	"io"
)

// 解析rdb
func (p *RDBParser) Parse() (err error) {
	var expire int64
	var flag byte
	var hasSelectDb bool
	if err = p.parseHeader(); err != nil {
		return err
	}
	for {
		select {
		case <-p.ctx.Done():
			return errors.New(ErrContextDone)
		default:
		}
		// Begin analyze
		flag, err = p.reader.ReadByte()
		if err != nil {
			break
		}
		if flag == FlagOpcodeIdle {
			b, _, err := p.loadLen()
			if err != nil {
				break
			}
			_ = int64(b) // lruIdle
			continue
		} else if flag == FlagOpcodeFreq {
			b, err := p.reader.ReadByte()
			if err != nil {
				break
			}
			_ = int64(b) // lfuIdle
			continue
		} else if flag == FlagOpcodeAux {
			// RDB 7 版本之后引入
			// redis-ver：版本号
			// redis-bits：OS Arch
			// ctime：RDB文件创建时间
			// used-mem：使用内存大小
			// repl-stream-db：在server.master客户端中选择的数据库
			// repl-id：当前实例 replication ID
			// repl-offset：当前实例复制的偏移量
			// lua：lua脚本
			key, err := p.loadString()
			if err != nil {
				err = errors.New("Parse Aux key failed: " + err.Error())
				break
			}
			val, err := p.loadString()
			if err != nil {
				err = errors.New("Parse Aux value failed: " + err.Error())
				break
			}
			if err = p.auxFields(key, val); err != nil {
				return err
			}
			continue
		} else if flag == FlagOpcodeResizeDB {
			// RDB 7 版本之后引入，详见 https://github.com/antirez/redis/pull/5039/commits/5cd3c9529df93b7e726256e2de17985a57f00e7b
			// 包含两个编码后的值，用于加速RDB的加载，避免在加载过程中额外的调整hash空间(resize)和rehash操作
			// 1.数据库的哈希表大小
			// 2.失效哈希表的大小
			dbSize, _, err := p.loadLen()
			if err != nil {
				err = errors.New("Parse ResizeDB size failed: " + err.Error())
				break
			}
			expiresSize, _, err := p.loadLen()
			if err != nil {
				err = errors.New("Parse ResizeDB size failed: " + err.Error())
				break
			}
			if err = p.resize(dbSize, expiresSize); err != nil {
				return err
			}
			continue
		} else if flag == FlagOpcodeExpireTimeMs {
			_, err := io.ReadFull(p.reader, p.buff)
			if err != nil {
				err = errors.New("Parse ExpireTime_ms failed: " + err.Error())
				break
			}
			expire = int64(binary.LittleEndian.Uint64(p.buff))
			continue
		} else if flag == FlagOpcodeExpireTime {
			_, err := io.ReadFull(p.reader, p.buff)
			if err != nil {
				err = errors.New("Parse ExpireTime failed: " + err.Error())
				break
			}
			expire = int64(binary.LittleEndian.Uint64(p.buff)) * 1000
			continue
		} else if flag == FlagOpcodeSelectDB {
			if hasSelectDb == true {
				continue
			}
			dbindex, _, err := p.loadLen()
			if err != nil {
				break
			}
			if err = p.selection(dbindex); err != nil {
				return err
			}
			hasSelectDb = false
			continue
		} else if flag == FlagOpcodeEOF {
			// TODO rdb_go_redis_parser checksum
			err = nil
			break
		}
		// Read key
		key, err := p.loadString()
		if err != nil {
			return err
		}
		// Read value
		if err := p.loadObject(key, flag, expire); err != nil {
			return err
		}
		expire = NotExpired
		//lfuIdle, lruIdle = -1, -1
	}
	return
}
