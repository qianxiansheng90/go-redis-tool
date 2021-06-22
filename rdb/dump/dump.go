/*
 *Descript:rdb dump
 */
package dump

import (
	"io"
	"net"
)

type RDBDumper struct {
	user             string
	password         string
	addr             string
	conn             net.Conn
	tlsEnable        bool
	keepAliveTimeout int64
	readTimeout      int
	rdbSize          int64
}
type DumperArg struct {
	RedisAddr        string
	RedisUser        string
	RedisPassword    string
	ReadTimeout      int
	KeepAliveTimeout int64
	TLSEnable        bool
}

func NewRDBDumper(arg DumperArg) *RDBDumper {
	if arg.ReadTimeout == 0 {
		arg.ReadTimeout = 5
	}
	if arg.KeepAliveTimeout == 0 {
		arg.KeepAliveTimeout = 5
	}
	return &RDBDumper{
		user:             arg.RedisUser,
		password:         arg.RedisPassword,
		addr:             arg.RedisAddr,
		conn:             nil,
		tlsEnable:        arg.TLSEnable,
		keepAliveTimeout: arg.KeepAliveTimeout,
		readTimeout:      arg.ReadTimeout,
	}
}

// 获取reader
func (r *RDBDumper) Reader() io.ReadCloser {
	return newNetReader(r.conn, r.readTimeout, r.rdbSize)
}

// 发起连接
func (r *RDBDumper) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
