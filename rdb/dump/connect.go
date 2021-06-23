/*
 *Descript:connect to redis
 */
package dump

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// 初始化连接
func (r *RDBDumper) InitConnection() (int64, error) {
	if err := r.openConn(); err != nil {
		return 0, err
	}
	if err := r.sendPSync(); err != nil {
		return 0, err
	}
	var rsp string
	for {
		b := []byte{0}
		r.conn.SetDeadline(time.Now().Add(time.Duration(r.readTimeout) * time.Second))
		if _, err := r.conn.Read(b); err != nil {
			return 0, errors.Wrap(err, "read sync response = "+rsp)
		}
		if len(rsp) == 0 && b[0] == '\n' {
			continue
		}
		rsp += string(b)
		if strings.HasSuffix(rsp, "\r\n") {
			break
		}
	}
	if rsp[0] != '$' {
		return 0, errors.Errorf("invalid sync response, rsp = '%s'", rsp)
	}
	n, err := strconv.ParseInt(rsp[1:len(rsp)-2], 10, 64)
	if err != nil || n <= 0 {
		return 0, errors.Wrap(err, fmt.Sprintf("invalid sync response = '%s', n = %d", rsp, n))
	}
	r.rdbSize = n
	return n, nil
}

// 发起连接
func (r *RDBDumper) openConn() error {
	d := &net.Dialer{
		KeepAlive: time.Duration(r.keepAliveTimeout) * time.Second,
	}
	var err error
	if r.tlsEnable {
		r.conn, err = tls.DialWithDialer(d, "tcp", r.addr, &tls.Config{InsecureSkipVerify: false})
	} else {
		r.conn, err = d.Dial("tcp", r.addr)
	}
	if err != nil {
		return errors.Wrap(err, "cannot connect to "+r.addr)
	}

	return r.auth()
}
func (r *RDBDumper) auth() error {
	if r.password == "" {
		return nil
	}
	if err := r.setConnDeadline(); err != nil {
		return err
	}
	_, err := r.conn.Write(MustEncodeToBytes(NewCommand("AUTH", r.password)))
	if err != nil {
		return errors.Wrap(err, "write auth command failed")
	}

	ret, err := ReadRESPEnd(r.conn)
	if err != nil {
		return errors.Wrap(err, "read auth response failed")
	}
	if strings.ToUpper(ret) != "+OK\r\n" {
		return errors.Errorf("auth failed[%v]", RemoveRESPEnd(ret))
	}
	return nil
}

func (r *RDBDumper) setConnDeadline() error {
	if err := r.conn.SetDeadline(time.Now().Add(time.Duration(r.readTimeout) * time.Second)); err != nil {
		return err
	}
	return nil
}
