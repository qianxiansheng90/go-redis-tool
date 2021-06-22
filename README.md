# go-redis-tool

# Introduct
A efficient and safe rdb dumper/parser in Golang.Interface is very friendly for developer.It also support common tool. 


### Usage of:
```
  -o string
    	set the output for parse. (default: out_file).can file-path or addr for redis

  -rdb string
    	<rdb-file-name>. For example: ./dump.rdb

  -addr string
        <host:port>. For example: 192.168.1.1:6379

  -type string
    	set the out type, support type: json、kv、redis. (default: kv)
```


### Feature
- Supports Redis from 2.8 to 5.0, all data types except module. Including:
    - String
    - Hash
    - List
    - Set
    - SortedSed
    - **Stream(Redis 5.0 new data type)**
- Support Dump RDB (Will)
- Support Parse RDB
- Support Load RDB to redis parallel(muti redis and very fast)
- **Support Context**

### Fork
parse code from [go-redis-parser](https://github.com/8090Lambert/go-redis-parser)

### Installation
#### via git
```
$ git clone https://github.com/8090Lambert/go-redis-parser.git && cd go-redis-parser
$ go install
```

#### via go
```
$ go get github.com/8090Lambert/go-redis-parser
```

# 介绍
go-redis-tool 是redis的RDB 转储和解析的工具，接口对开发者非常友好。同样也提供了一个简单的工具


### 使用方式
```
  -action string
        <parse/load/dump/trans>.parse rdb/load rdb to redis/dump rdb from redis (default "parse")
  -from_addr string
        <redis-host:redis-port>.dump from redis addr.For example:192.168.1.1:6379
  -out_file string
        <file-path/redis-host:redis-port>.For example: ./dump.rdb.csv (default "./out_file")
  -parse_type string
        <kv/json/none>. (default "kv")
  -rdb string
        <rdb-file-name>. For example: ./dump.rdb
  -to_addr string
        <redis-host:redis-port>.dump to redis addr.For example:192.168.1.1:6379
```


### 特性
- 支持redis2.8-5.0,数据类型包括:
    - String
    - Hash
    - List
    - Set
    - SortedSed
    - **Stream(Redis 5.0 new data type)**
    - Module(Not support)
- 支持 dump rdb (Will)
- 支持解析rdb
- 支持将RDB解析并行加载到Redis中(支持多个redis入口并且速度非常快)
- **支持 Context**

### Fork
解析RDB的核心代码来自[go-redis-parser](https://github.com/8090Lambert/go-redis-parser)

### 安装使用
#### via git
```
$ git clone https://github.com/8090Lambert/go-redis-parser.git && cd go-redis-parser
$ go install
```

#### via go
```
$ go get github.com/8090Lambert/go-redis-parser
```