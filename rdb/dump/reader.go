package dump

import (
	"io"
	"net"
	"time"
)

// netReader结构体
type netReader struct {
	conn        net.Conn
	readTimeout int
	rdbSize     int64
	readSize    int64
}

// 新建一个netReader
func newNetReader(conn net.Conn, readTimeout int, rdbSize int64) io.ReadCloser {
	return &netReader{
		conn:        conn,
		readTimeout: readTimeout,
		rdbSize:     rdbSize,
	}
}

// 读取数据
func (r *netReader) Read(p []byte) (int, error) {
	if err := r.conn.SetDeadline(time.Now().Add(time.Duration(r.readTimeout) * time.Second)); err != nil {
		return 0, err
	}
	var pLen = int64(len(p))
	var readLen = pLen
	diff := r.rdbSize - r.readSize
	if diff < pLen {
		readLen = diff
	}
	size, err := r.conn.Read(p[:readLen])
	r.readSize += int64(size)
	if err == nil && readLen != pLen {
		err = io.EOF
	}
	return size, err
}

// 关闭
func (r *netReader) Close() error {
	if r.conn == nil {
		return nil
	}
	return r.conn.Close()
}
