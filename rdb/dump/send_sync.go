/*
 *Descript:send sync to redis
 */
package dump

import (
	"fmt"
)

// 发送psync
func (r *RDBDumper) sendPSync() error {
	if _, err := r.conn.Write(MustEncodeToBytes(NewCommand("sync"))); err != nil {
		return fmt.Errorf("%s %s ", "write sync command failed", err.Error())
	}
	return nil
}
