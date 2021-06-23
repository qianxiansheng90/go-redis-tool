/*
 *Descript:send sync to redis
 */
package dump

import (
	"github.com/pkg/errors"
)

// 发送psync
func (r *RDBDumper) sendPSync() error {
	if err := r.setConnDeadline(); err != nil {
		return err
	}
	if _, err := r.conn.Write(MustEncodeToBytes(NewCommand("sync"))); err != nil {
		return errors.Wrap(err, "write sync command failed")
	}
	return nil
}
