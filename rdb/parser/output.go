
/*
DESCRIPTION:rdb 解析list
*/
package parser

// kv格式
const (
	OutKVFormat = "type:%s|key:%s|value:%s|expire:%d"
)

// json格式
type JSONFormat struct {
	KeyType string      `json:"type"`
	Key     string      `json:"key"`
	Value   interface{} `json:"value"`
}
type JSONExpireFormat struct {
	KeyType string      `json:"type"`
	Key     string      `json:"key"`
	Value   interface{} `json:"value"`
	Expire  int64       `json:"expire"`
}
