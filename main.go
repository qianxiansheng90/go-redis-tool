package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/qianxiansheng90/go-redis-tool/rdb/dump"
	"github.com/qianxiansheng90/go-redis-tool/rdb/load"
	"github.com/qianxiansheng90/go-redis-tool/rdb/parser"
)

const (
	parseRDBToKV   = "kv"
	parseRDBToJson = "json"
	parseRDBToNone = "none"
	actionDump     = "dump"
	actionLoad     = "load"
	actionParse    = "parse"
	actionTrans    = "trans"
	actionInfo     = "info"
)

var (
	action            = flag.String("action", actionDump, "<parse/load/dump/trans/info>.parse rdb file/load rdb file to redis/dump rdb from redis/dump rdb from redis and load to redis")
	rdbFile           = flag.String("rdb", "", "<rdb-file-name>. For example: ./dump.rdb")
	fromRedisAddr     = flag.String("from_addr", "127.0.0.1:6379", "<redis-host:redis-port>.dump from redis addr.For example:192.168.1.1:6379")
	fromRedisAuthPass = flag.String("from_auth", "", "connect to from_addr dump rdb when set requirepass")
	toRedisAddr       = flag.String("to_addr", "", "<redis-host:redis-port>.load rdb to redis addr.For example:192.168.1.1:6379")
	toRedisAuthUser   = flag.String("to_auth_user", "", "connect to to_addr with account username")
	toRedisAuthPass   = flag.String("to_auth_pass", "", "connect to to_addr with account password")
	parseType         = flag.String("parse_type", "none", "<csv/json/none>.")
	outDst            = flag.String("out_file", "./out_file", "<file-path/redis-host:redis-port>.For example: ./dump.rdb.csv")
	outBigKey         = flag.Bool("big_key", false, "print big key")
)

func main() {
	flag.Parse()
	switch *action {
	case actionDump:
		if *fromRedisAddr == "" {
			fmt.Println("need from_addr")
			return
		}
		switch *parseType {
		case parseRDBToKV, parseRDBToJson, parseRDBToNone:
			dumpRedisRDBToFile(*fromRedisAddr, *outDst, *parseType, *fromRedisAuthPass)
		default:
			fmt.Println("not support parse_type")
		}
	case actionLoad:
		if *rdbFile == "" {
			fmt.Println("need rdb")
			return
		}
		if *toRedisAddr == "" {
			fmt.Println("need to_addr")
			return
		}

		loadRDBFileToRedis(*rdbFile, *toRedisAddr, *toRedisAuthUser, *toRedisAuthPass)
	case actionParse:
		if *rdbFile == "" {
			fmt.Println("need rdb")
			return
		}
		switch *parseType {
		case parseRDBToKV, parseRDBToJson, parseRDBToNone:
			parseRDBFile(*rdbFile, *parseType, *outDst)
		default:
			fmt.Println("not support parse_type")
		}
	case actionTrans:
		if *fromRedisAddr == "" {
			fmt.Println("need from_addr")
			return
		}
		if *toRedisAddr == "" {
			fmt.Println("need to_addr")
			return
		}
		transRedisRDBToRedis(*fromRedisAddr, *toRedisAddr, *toRedisAuthUser, *toRedisAuthPass)
	case actionInfo:
		if *rdbFile == "" {
			fmt.Println("need rdb")
			return
		}
		getRDBInfo(*rdbFile, *outBigKey)
	default:
		fmt.Println("not support action")
		return
	}
}

func getRDBInfo(rdbFile string, bigKey bool) {
	info, err := load.GetRDBFileInfo(context.TODO(), rdbFile, load.GetRDBInfoArg{
		OnlyRDBInfo:   !bigKey,
		KeyStatistics: false,
		BigKey:        bigKey,
		BigKeyArg: load.BigKeyArg{
			ValueSize: 1024,
			TypeVal:   map[string]load.BigKey{},
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("redis-version:%s bgsaveTime:%d usedMem:%d\n", info.RedisVersion, info.RedisCTime, info.RedisUsedMemory)
	if bigKey {
		data, err := json.Marshal(info.KeyStatistics)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(string(data))
	}
}

// 解析rdb文件
func parseRDBFile(filePath, outType, dst string) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()
	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer dstFile.Close()
	switch outType {
	case parseRDBToKV, parseRDBToJson:
		if _, err = load.ParseRDBOutJson(context.TODO(), file, dstFile, parser.ParseArg{}); err != nil {
			fmt.Println(err)
		}
	case parseRDBToNone:
		if _, err = io.Copy(dstFile, file); err != nil {
			fmt.Println(err)
		}
	default:
		fmt.Println("not support type")
	}
}

// 将redis的rdb导出到文件
func dumpRedisRDBToFile(fromRedisAddr, rdbFile, outType, userPass string) {
	dumper := dump.NewRDBDumper(dump.DumperArg{
		RedisAddr:        fromRedisAddr,
		RedisUser:        "",
		RedisPassword:    userPass,
		ReadTimeout:      0,
		KeepAliveTimeout: 0,
		TLSEnable:        false,
	})
	defer dumper.Close()
	_, err := dumper.InitConnection()
	if err != nil {
		fmt.Println(err)
		return
	}
	reader := dumper.Reader()
	dstFile, err := os.OpenFile(rdbFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	switch outType {
	case parseRDBToKV, parseRDBToJson:
		if _, err = load.ParseRDBOutJson(context.TODO(), reader, dstFile, parser.ParseArg{}); err != nil {
			fmt.Println(err)
		}
	case parseRDBToNone:
		if _, err = io.Copy(dstFile, reader); err != nil {
			fmt.Println(err)
		}
	}
}

// 加载rdb文件到redis
func loadRDBFileToRedis(rdbFile, toRedisAddr, userName, userPass string) {
	file, err := os.Open(rdbFile)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()
	loader, err := load.NewRDBLoad(context.TODO(), file, load.LoadArg{
		Addr:     []string{toRedisAddr},
		Username: userName,
		Password: userPass,
	}, parser.ParseArg{})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer loader.Close()
	if err = loader.Run(); err != nil {
		fmt.Println(err)
	}
}

// 从redis将rdb导出到另一个redis中
func transRedisRDBToRedis(fromRedisAddr, toRedisAddr, userName, userPass string) {
	dumper := dump.NewRDBDumper(dump.DumperArg{
		RedisAddr:        fromRedisAddr,
		RedisUser:        userName,
		RedisPassword:    userPass,
		ReadTimeout:      0,
		KeepAliveTimeout: 0,
		TLSEnable:        false,
	})
	defer dumper.Close()
	if _, err := dumper.InitConnection(); err != nil {
		fmt.Println(err)
		return
	}
	reader := dumper.Reader()
	loader, err := load.NewRDBLoad(context.TODO(), reader, load.LoadArg{
		Addr: []string{toRedisAddr},
	}, parser.ParseArg{})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer loader.Close()
	if err = loader.Run(); err != nil {
		fmt.Println(err)
		return
	}
}
