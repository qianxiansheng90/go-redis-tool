# go-redis-tool

# Introduct
A efficient and safe rdb dumper/parser in Golang.Interface is very friendly for developer.It also support common tool. 


### Usage of:
```
  -action string
        <parse/load/dump/trans>.parse rdb file/load rdb file to redis/dump rdb from redis/dump rdb from redis and load to redis (default "dump")
        
  -from_addr string
        <redis-host:redis-port>.dump from redis addr.For example:192.168.1.1:6379 (default "127.0.0.1:6379")

  -from_auth string
        connect to from_addr dump rdb when set requirepass

  -out_file string
        <file-path/redis-host:redis-port>.For example: ./dump.rdb.csv (default "./out_file")

  -parse_type string
        <csv/json/none>. (default "none")

  -rdb string
        <rdb-file-name>. For example: ./dump.rdb

  -to_addr string
        <redis-host:redis-port>.load rdb to redis addr.For example:192.168.1.1:6379

  -to_auth_pass string
        connect to to_addr with account password

  -to_auth_user string
        connect to to_addr with account username
```


### Feature
- Supports Redis from 2.8 to 5.0, all data types except module. Including:
    - String
    - Hash
    - List
    - Set
    - SortedSed
    - **Stream(Redis 5.0 new data type)**
- Support Dump RDB
- Support Parse RDB
- Support Load RDB to redis parallel(support muti redis and very fast)
- **Support Context**

### Reference
parse code from [go-redis-parser](https://github.com/8090Lambert/go-redis-parser)

dump code from [RedisShake](https://github.com/alibaba/RedisShake)

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
        指令,可选项为<parse/load/dump/trans>.默认为dump
        parse:解析rdb文件为指定的格式
        load:解析rdb文件并将rdb加载到redis中
        dump:从redis导出rdb,并按照指定的格式写入到文件
        trans:从redis导出rdb并加载到redis中,期间不落盘
        info:输出RDB信息和大key信息

  -from_addr string
        指令为dump/trans有效.源redis的地址,格式为ip:port,默认127.0.0.1:6379

  -from_auth string
        指令为dump/trans有效.连接源redis的需要的密码

  -out_file string
        指令为dump/parse有效.结果写入到哪个文件,默认为./out_file

  -parse_type string
        指令为dump/parse有效.解析rdb文件为那种格式,可选项:kv|json|none(原rdb文件格式).默认为none

  -rdb string
        指令为parse/load/info有效.需要解析rdb的文件全路径,默认为./dump.rdb

  -to_addr string
        指令为load/trans有效.目标redis的地址,默认空

  -to_auth_pass string
        指令为load/trans有效.连接目标redis的地址需要的密码,默认为空.

  -to_auth_user string
        指令为load/trans有效.连接目标redis的地址需要的用户名(需要redis6.0以上),默认为空.

  -big_key bool
        指令为info有效.输出大key信息,默认为false.
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
- 支持 dump rdb
- 支持解析rdb
- 支持将RDB解析并行加载到Redis中(支持多个redis入口并且速度非常快)
- **支持 Context**

### 参考
解析RDB的部分代码参考[go-redis-parser](https://github.com/8090Lambert/go-redis-parser)

dump RDB的部分代码参考[RedisShake](https://github.com/alibaba/RedisShake)

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
