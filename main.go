package main

import (
	"context"
	"flag"
	"os"

	"github.com/qianxiansheng90/go-redis-parser/rdb/load"
	"github.com/qianxiansheng90/go-redis-parser/rdb/parser"
)

var (
	//flag.StringVar(&aofFile, "aof", "", "file.aof. For example: ./appendonly.aof\n")
	rdbFile   = flag.String("rdb", "", "<rdb-file-name>. For example: ./dump.rdb")
	redisAddr = flag.String("addr", "", "<redis-host:redis-port>.For example:192.168.1.1:6379")
	outType   = flag.String("type", "csv", "<csv/json/redis>.where parse rdb output to")
	outDst    = flag.String("o", "./out_file", "<file-path/redis-host:redis-port>.For example: ./dump.rdb.csv")
)

func main() {
	flag.Parse()
	switch *outType {
	case "csv", "json", "redis":
	default:
		panic("not support out format")
	}

	if *rdbFile != "" { // 解析rdb文件
		parseRDBFile(*rdbFile, *outType, *outDst)
		return
	}
	if *redisAddr != "" { // 从redis中获取rdb,并解析
		parseRedisRDB(*rdbFile, *outType, *outDst)
	}

}

func parseRDBFile(filePath, outType, dst string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if outType == "csv" {
		dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		defer dstFile.Close()
		load.ParseRDBOutKV(context.TODO(), file, dstFile, parser.ParseArg{})
	}
	switch outType {
	case "kv":
		dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		defer dstFile.Close()
		if _, err = load.ParseRDBOutKV(context.TODO(), file, dstFile, parser.ParseArg{}); err != nil {
			panic(err)
		}
	case "json":
		dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		defer dstFile.Close()
		if _, err = load.ParseRDBOutJson(context.TODO(), file, dstFile, parser.ParseArg{}); err != nil {
			panic(err)
		}
	case "redis":
		loader, err := load.NewRDBLoad(context.TODO(), file, load.LoadArg{
			Addr: []string{dst},
		}, parser.ParseArg{})
		if err != nil {
			panic(err)
		}
		defer loader.Close()
		if err = loader.Run(); err != nil {
			panic(err)
		}
	default:
		panic("not support type")
	}
}

func parseRedisRDB(src, outType, dst string) {
	return
}
