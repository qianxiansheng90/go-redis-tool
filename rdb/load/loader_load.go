/*
 *Descript:将object输出到redis
 */
package load

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/qianxiansheng90/go-redis-parser/log_interface"
	"github.com/qianxiansheng90/go-redis-parser/rdb/parser"
)

const (
	invalidExp   = 0
	TimeFormat   = "2006-01-02 15:04:05"     // 一般时间格式
	TimeFormatMS = "2006-01-02 15:04:05.000" // 时间带毫秒
)

type LoadResult struct {
	KeyCount      map[string]int64
	TotalKeyCount int64
}

var (
	intervalTime     = 1 * time.Millisecond
	intervalLongTime = 10 * time.Millisecond
)

// out command format
func (l *RedisLoader) loadCommand(ctx context.Context, object parser.TypeObject) error {
	l.totalKeyCount++
	switch object.Type() {
	case parser.SelectionDB{}.Type(): // 需要等待所有的conn切换db
		return l.changeDB(object)
	default: // 随机发送数据
		return l.sendData(object)
	}
}

// 所有连接切换db
func (l *RedisLoader) changeDB(object parser.TypeObject) error {
	for _, c := range l.redisConnPool {
		for {
			if err := l.checkExit(); err != nil { // 检查是否应该退出
				return err
			}
			select {
			case c.changeDBChan <- object: // 发送数据
				goto end
			case <-time.After(intervalTime): // 超时
			}
		}
	end:
	}
	return nil
}

// 发送数据
func (l *RedisLoader) sendData(object parser.TypeObject) error {
	for {
		if err := l.checkExit(); err != nil { // 检查是否应该退出
			return err
		}
		select {
		case l.dataChan <- object: // 发送数据
			goto end
		case <-time.After(intervalTime): // 超时
		}
	}
end:
	return nil
}

// 限速
func (l *RedisLoader) limit() error {
	if l.limiter == nil {
		return nil
	}
	for {
		if err := l.checkExit(); err != nil { // 检查是否应该退出
			return err
		}
		if l.limiter.Allow() == false {
			time.Sleep(intervalTime)
			continue
		}
		return nil

	}
}

// 开启并行导入goroutine
func (l *RedisLoader) loadCommandGoroutine(ctx context.Context, idx int, changeDBChan, loadDataChan chan parser.TypeObject) {
	l.Log("start goroutine %d", idx)
	var conn *redis.Client
	var err error
	defer func() { // 关闭连接
		if conn != nil {
			conn.Close()
		}
		l.lock.Lock()
		defer l.lock.Unlock()
		l.runningGoroutine--
		l.Log("%d:goroutine end", idx)
	}()
	conn, err = getRedisConn(ctx, idx, l.loadArg)
	if err != nil {
		l.err = err
		return
	}
	var dbNum uint64 = 0
	var ok bool
	var objects = make([]parser.TypeObject, l.pipeLineCmdLen)
	var objIdx = 0
	for {
		if err := l.checkExit(); err != nil { // 检查是否应该退出
			return
		}

		select {
		case changeObj := <-changeDBChan: // 需要将之前的数据全部写入
			if objIdx > 0 {
				if err := l.handleRedisKeyPipeline(ctx, idx, dbNum, conn, objects[:objIdx]); err != nil {
					l.err = err
					return
				}
				objIdx = 0
			}
			_, val, _ := changeObj.Command()
			dbNum, ok = val[0].(uint64)
			if ok == false {
				l.err = errors.New("internal error dbsize value")
				return
			}
		case obj, isOpen := <-l.dataChan: // 收到命令
			if !isOpen { // channel已经关闭
				if objIdx > 0 { // 如果还有数据需要导入
					if err := l.handleRedisKeyPipeline(ctx, idx, dbNum, conn, objects[:objIdx]); err != nil {
						l.err = err
						return
					}
				}
				return
			}
			objects[objIdx] = obj
			objIdx++
			if objIdx >= l.pipeLineCmdLen { // 缓存量如果要超过限制
				if err := l.handleRedisKeyPipeline(ctx, idx, dbNum, conn, objects[:objIdx]); err != nil {
					l.err = err
					return
				}
				objIdx = 0
			}
		case <-time.After(intervalTime): // 超时
		}
	}
}

// 批量处理key
func (l *RedisLoader) handleRedisKeyPipeline(ctx context.Context, idx int, dbNum uint64, conn *redis.Client, objects []parser.TypeObject) error {
	if l.loadArg.DelMode == true { // 删除数据模式
		return l.delRedisKeyPipelineRetry(ctx, dbNum, conn, objects)
	}
	return l.loadRedisCommandPipelineRetry(ctx, idx, dbNum, conn, objects)
}

// 批量删除key:可以重试
func (l *RedisLoader) delRedisKeyPipelineRetry(ctx context.Context, dbNum uint64, conn *redis.Client, objects []parser.TypeObject) (err error) {
	for i := 0; i < l.maxRetryPerCmd; i++ {
		if err = l.delRedisKeyPipeline(ctx, dbNum, conn, objects); err == nil {
			return nil
		}
	}
	return
}

// 批量删除key
func (l *RedisLoader) delRedisKeyPipeline(ctx context.Context, dbNum uint64, conn *redis.Client, objects []parser.TypeObject) error {
	if len(objects) == 0 {
		return nil
	}
	var pipe = conn.Pipeline()
	selectCmd := pipe.Do(ctx, "select", dbNum)
	if selectCmd.Err() != nil {
		return errors.Wrap(selectCmd.Err(), "delete select")
	}
	for _, object := range objects {
		key, val, _ := object.Command()
		errString := fmt.Sprintf("del %s key %s len %d value %+v ", object.Type(), key, len(val), val)
		if status := pipe.Del(ctx, key); status.Err() != nil {
			return errors.Wrap(status.Err(), errString)
		}
	}
	resultArr, err := pipe.Exec(ctx)
	if err != nil {
		return errors.Wrap(err, "delete pipeline exec")
	}
	for _, result := range resultArr {
		if result.Err() != nil {
			return errors.Wrap(err, "delete pipeline result")
		}
	}
	return nil
}

// 批量导入命令:可以重试
func (l *RedisLoader) loadRedisCommandPipelineRetry(ctx context.Context, idx int, dbNum uint64, conn *redis.Client, objects []parser.TypeObject) error {
	var err error
	for i := 0; i < l.maxRetryPerCmd; i++ {
		// 如果加载数据失败则先删除key再重试加载数据
		err = l.loadRedisCommandPipeline(ctx, dbNum, conn, objects)
		if err != nil {
			l.Log("%d:load command error %s", idx, err.Error())
			if err = l.delRedisKeyPipelineRetry(ctx, dbNum, conn, objects); err != nil {
				return err
			}
		}
		return nil
	}
	return err
}

// 批量导入命令
func (l *RedisLoader) loadRedisCommandPipeline(ctx context.Context, dbNum uint64, conn *redis.Client, objects []parser.TypeObject) error {
	if len(objects) == 0 {
		return nil
	}
	var pipe = conn.Pipeline()
	selectCmd := pipe.Do(ctx, "select", dbNum)
	if selectCmd.Err() != nil {
		return errors.Wrap(selectCmd.Err(), "select")
	}
	for _, object := range objects {
		key, val, exp := object.Command()
		if l.loadArg.NoExpTime == false && l.loadArg.ExpTimeShiftMS != 0 && exp.Equal(time.Time{}) == false { // 设置了过期时间偏移
			exp = exp.Add(time.Duration(l.loadArg.ExpTimeShiftMS) * time.Millisecond)
		}
		errString := fmt.Sprintf("cmd %s key %s len %d value %+v", object.Type(), key, len(val), val)
		switch object.Type() {
		case parser.StringObject{}.Type():
			var expDuration time.Duration = invalidExp
			if exp.Equal(time.Time{}) == false {
				expDuration = exp.Sub(time.Now())
			}
			if status := pipe.Set(ctx, key, val[0], expDuration); status.Err() != nil {
				return errors.Wrap(status.Err(), errString)
			}
		case parser.ListObject{}.Type():
			if status := pipe.LPush(ctx, key, val...); status.Err() != nil {
				return errors.Wrap(status.Err(), errString)
			}
		case parser.HashMap{}.Type():
			if status := pipe.HMSet(ctx, key, val...); status.Err() != nil {
				return errors.Wrap(status.Err(), errString)
			}
		case parser.RedisStream{}.Type():
			if len(val) == 0 {
				continue
			}
			xadds, ok := val[0].(parser.XAdds)
			if ok == false {
				return fmt.Errorf("convert stream data error")
			}
			sort.Sort(xadds)
			for _, k := range xadds {
				if k.HasDelete == true && l.loadArg.SaveStreamDelVal == false {
					continue
				}
				status := pipe.XAdd(ctx, k.XaddArg)
				if status.Err() != nil {
					return errors.Wrap(status.Err(), errString)
				}
			}

		case parser.Set{}.Type():
			if status := pipe.SAdd(ctx, key, val...); status.Err() != nil {
				return errors.Wrap(status.Err(), errString)
			}
		case parser.SortedSet{}.Type():
			var zsetVal []*redis.Z
			for _, k := range val {
				z, ok := k.(parser.SortedSetEntry)
				if ok == false {
					return fmt.Errorf(parser.ErrIllegalZSetData)
				}
				zsetVal = append(zsetVal, &redis.Z{
					Score:  z.Score,
					Member: z.Field,
				})
			}
			if status := pipe.ZAdd(ctx, key, zsetVal...); status.Err() != nil {
				return errors.Wrap(status.Err(), errString)
			}
		case parser.SelectionDB{}.Type():
		case parser.ResizeDB{}.Type():
		case parser.AuxField{}.Type():
		default:
			// continue
			return fmt.Errorf(parser.ErrUnknownDataFormat)
		}
		if exp.Equal(time.Time{}) {
			continue
		}
		if l.loadArg.NoExpTime == true { // 忽略过期时间
			continue
		}
		expStatus := pipe.PExpire(ctx, key, exp.Sub(time.Now()))
		if expStatus.Err() != nil {
			return errors.Wrap(expStatus.Err(), errString)
		}
	}
	resultArr, err := pipe.Exec(ctx)
	if err != nil {
		return errors.Wrap(selectCmd.Err(), "pipeline exec")
	}
	for _, result := range resultArr {
		if result.Err() != nil {
			return errors.Wrap(selectCmd.Err(), "pipeline result")
		}
	}
	return nil
}

//钩子处理函数
type hook struct {
	logTimeout bool
	startTime  time.Time
	logger     log_interface.Logger
}

func (h *hook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	h.startTime = time.Now()
	return ctx, nil
}
func (h *hook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	spend := time.Now().Sub(h.startTime).Milliseconds()
	if spend > 1000 && h.logTimeout == true && h.logger != nil {
		h.logger.Debugf("cmd:%5s len %6d %20s %dms", cmd.Name(), len(cmd.Args()), h.startTime.Format(TimeFormat), spend)
	}
	return nil
}

func (h *hook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return ctx, nil
}
func (h *hook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	return nil
}

// 检查是否需要退出
func (l *RedisLoader) checkExit() (err error) {
	if l.err != nil {
		return l.err
	}
	select {
	case <-l.ctx.Done():
		return errors.New("context done")
	case <-l.ctx.Done():
		return errors.New("context done")
	default:
		return
	}
}

// 获取结果
func (l *RedisLoader) LoadResult() LoadResult {
	return LoadResult{
		TotalKeyCount: l.totalKeyCount,
	}
}

// 打印日志
func (l *RedisLoader) Log(fmtString string, val ...interface{}) {
	if l.loadArg.Debug == true && l.loadArg.Logger != nil {
		l.loadArg.Logger.Debugf("[%s] %s", time.Now().Format(TimeFormatMS), fmt.Sprintf(fmtString, val...))
	}
}
