/*
 *Descript:rdb加载器,加载到redis中
 */
package load

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/time/rate"

	"github.com/qianxiansheng90/go-redis-parser/log_interface"
	"github.com/qianxiansheng90/go-redis-parser/rdb/parser"
)

// 解析参数结构体
type LoadArg struct {
	Addr             []string             // redis地址
	Username         string               // redis的连接用户
	Password         string               // redis连接密码
	DB               int                  // 连接redis的db
	DialTimeout      int                  // 超时时间(ms)
	ReadTimeout      int                  // 读超时时间(ms)
	WriteTimeout     int                  // 写超时时间(ms)
	LoadParallel     int                  // 导入到redis中的并行线程数
	Speed            int                  // 限速
	NoExpTime        bool                 // 忽略过期时间
	ExpTimeShiftMS   int                  // 过期时间偏移多少ms:正数是向前,负数向后
	SaveStreamDelVal bool                 // 保留stream中删除的val
	MaxRetryPerCmd   int                  // 每个命令最多重试多少次
	PipeLineCmdLen   int                  // 每个批次多少个命令
	Debug            bool                 // debug 模式
	Logger           log_interface.Logger // 打印日志
	DelMode          bool                 // 删除模式
}

// 加载器
type RedisLoader struct {
	loadArg          LoadArg                // 加载器参数
	redisConnPool    []redisConnPool        // redis连接池
	dataChan         chan parser.TypeObject // 数据channel
	ctx              context.Context        // 结束并行线程
	cancel           context.CancelFunc     // 取消函数
	limiter          *rate.Limiter          // 限速器
	lock             *sync.RWMutex          // 运行锁
	runningGoroutine int                    // 当前运行的线程
	err              error                  // 错误信息
	parser           *parser.RDBParser      // 解析器
	parserArg        parser.ParseArg        // 解析器参数
	totalKeyCount    int64                  // 加载的key的数量
	maxRetryPerCmd   int                    // 每个命令最多重试多少次
	pipeLineCmdLen   int                    // 每个批次多少个命令
}

// 连接池
type redisConnPool struct {
	redisConn    *redis.Conn
	changeDBChan chan parser.TypeObject
}

// 创建一个加载器
func NewRDBLoad(ctx context.Context, reader io.Reader, arg LoadArg, pArg parser.ParseArg) (*RedisLoader, error) {
	loaderCtx, loaderCancel := context.WithCancel(context.Background())
	var limiter *rate.Limiter
	if arg.Speed > 0 {
		var intervalMicroSecond = 1000000 / arg.Speed
		var limitTaskPerSecond = rate.Every(time.Duration(intervalMicroSecond) * time.Microsecond)
		limiter = rate.NewLimiter(limitTaskPerSecond, arg.Speed) // 限速器
	}
	if arg.MaxRetryPerCmd <= 0 {
		arg.MaxRetryPerCmd = 3
	}
	if arg.PipeLineCmdLen <= 0 {
		arg.PipeLineCmdLen = 10
	}
	var err error
	loadDataChan := make(chan parser.TypeObject)
	var l = RedisLoader{
		loadArg:          arg,
		redisConnPool:    []redisConnPool{},
		dataChan:         loadDataChan,
		ctx:              loaderCtx,
		cancel:           loaderCancel,
		limiter:          limiter,
		lock:             &sync.RWMutex{},
		runningGoroutine: 0,
		err:              nil,
		parser:           &parser.RDBParser{},
		parserArg:        pArg,
		maxRetryPerCmd:   arg.MaxRetryPerCmd,
		pipeLineCmdLen:   arg.PipeLineCmdLen,
	}
	l.parser, err = parser.NewRDBParse(ctx, reader, l.loadCommand, l.closeLoader, pArg)
	if err != nil {
		return &l, err
	}

	return &l, nil
}

// 连接redis
func (l *RedisLoader) Run() (err error) {
	if err := l.getRedisConn(l.ctx, l.dataChan, l.loadArg); err != nil {
		return err
	}
	return l.parser.Parse()
}

// 关闭
func (l *RedisLoader) Close() error {
	return l.closeLoader(l.ctx)
}

// 连接redis
func (l *RedisLoader) getRedisConn(ctx context.Context, loadDataChan chan parser.TypeObject, arg LoadArg) (err error) {
	if arg.LoadParallel <= 0 {
		arg.LoadParallel = 1
	}

	redisCPool := make([]redisConnPool, arg.LoadParallel)
	l.runningGoroutine = arg.LoadParallel
	for i := 0; i < arg.LoadParallel; i++ {
		changeDBChan := make(chan parser.TypeObject)
		redisCPool[i] = redisConnPool{
			redisConn:    nil,
			changeDBChan: changeDBChan,
		}
		go l.loadCommandGoroutine(ctx, i, changeDBChan, loadDataChan)
	}
	l.redisConnPool = redisCPool
	return
}

// 获取redis连接
func getRedisConn(ctx context.Context, idx int, arg LoadArg) (*redis.Client, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:         arg.Addr[idx%len(arg.Addr)],
		Username:     arg.Username,
		Password:     arg.Password, // no password set
		DB:           arg.DB,       // use default DB
		DialTimeout:  time.Duration(arg.DialTimeout) * time.Millisecond,
		ReadTimeout:  time.Duration(arg.ReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(arg.WriteTimeout) * time.Millisecond,
		PoolSize:     arg.LoadParallel,
	})
	var h = hook{
		logTimeout: arg.Debug,
		startTime:  time.Time{},
		logger:     arg.Logger,
	}
	redisClient.AddHook(&h)
	return redisClient, redisClient.Ping(ctx).Err()
}

// 关闭
func (l *RedisLoader) closeLoader(ctx context.Context) (err error) {
	l.err = io.EOF
	if l.dataChan != nil {
		close(l.dataChan)
		for {
			if l.runningGoroutine == 0 {
				break
			}
			time.Sleep(intervalLongTime)
		}
	}

	if l.cancel != nil {
		l.cancel()
	}
	for _, k := range l.redisConnPool {
		if k.redisConn != nil {
			err = k.redisConn.Close()
		}
	}
	return
}
