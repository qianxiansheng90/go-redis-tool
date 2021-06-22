package dump

import (
	"io"
	"net"
	"time"
)

type netReader struct {
	reader      net.Conn
	readTimeout int
	rdbSize     int64
	readSize    int64
}

func newNetReader(reader net.Conn, readTimeout int, rdbSize int64) io.ReadCloser {
	return &netReader{
		reader:      reader,
		readTimeout: readTimeout,
		rdbSize:     rdbSize,
	}
}
func (r *netReader) Read(p []byte) (int, error) {
	if err := r.reader.SetDeadline(time.Now().Add(time.Duration(r.readTimeout) * time.Second)); err != nil {
		return 0, err
	}
	var pSize = int64(len(p))
	diff := r.rdbSize - r.readSize
	if diff < int64(pSize) {
		pSize = diff
	}
	size, err := r.reader.Read(p[:pSize])
	r.readSize += int64(size)
	if err == nil && pSize != int64(len(p)) {
		err = io.EOF
	}
	return size, err
}

func (r *netReader) Close() error {
	if r.reader == nil {
		return nil
	}
	return r.reader.Close()
}
