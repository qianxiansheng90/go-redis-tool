/*
 *Descript:抽象接口
 */
package parser

import "time"

// interface type object
type TypeObject interface {
	Value() string                               // value
	ValueLen() uint64                            // value length
	Key() string                                 // Key
	String() string                              // Print string
	Type() string                                // Redis data type
	ConcreteSize() uint64                        // Data bytes size, except metadata
	Command() (string, []interface{}, time.Time) // out command
	JSON() ([]byte, error)                       // out json data
	KV() ([]byte, error)                         // out key value data
}
