/*
 *Descript:aof解析
 */
package aof

import (
	"bytes"
	"encoding/binary"
	"io"
	"strconv"

	"github.com/pkg/errors"
)

const (
	Doller   = 36 // $
	Return   = 10 // \n
	Carriage = 13 // \r
	Star     = 42 // *
	Comm     = 58 // :
	Minus    = 45 // -

	ErrNotForkAofFormat    = "not fork aof format"
	ErrDataLineFormat      = "data line format"
	ErrDataLen             = "data len"
	ErrCommandLenFormat    = "command len format"
	ErrAofLineLengthFormat = "aof line length format"
	ErrAofLineEndFormat    = "aof line end format"
)

// 获取命令
func (a *AofParser) GetNextAofCommand() ([]string, error) {
	return a.getNextCommand()
}

// 返回最后一次获取命令的offset
func (a *AofParser) GetLastCommandOffset() int64 {
	return a.nextCmdOffset
}

// 重置文件的offset
func (a *AofParser) ResetFileOffset(offset int64) (err error) {
	if a.file == nil {
		return errors.New("file not open")
	}
	a.offset, err = a.file.Seek(offset, io.SeekStart)
	a.reader.Reset(a.file)
	return
}

// 设置offset
func (a *AofParser) SetOffset(offset int64) {
	a.offset = offset
}

// 获取下一组命令
func (a *AofParser) getNextCommand() ([]string, error) {
	data, err := a.getLine()
	if err != nil {
		return nil, err
	}
	if len(data) < 2 {
		return nil, errors.New(ErrDataLineFormat)
	}
	var commandLen int
	if data[0] == Star { // 长度
		commandLen, err = BytesToInt(data[1:])
	}
	var commands []string
	for i := 0; i < commandLen; i++ {
		cmdString, err := a.getCommandString()
		if err != nil {
			return nil, err
		}
		commands = append(commands, cmdString)
	}
	a.nextCmdOffset = a.offset
	return commands, nil
}

// 获取命令
func (a *AofParser) getCommandString() (string, error) {
	data, err := a.getLine()
	if err != nil {
		return "", err
	}
	if len(data) < 2 {
		return "", errors.New(ErrDataLineFormat)
	}
	var commandLen int
	if data[0] == Doller { // 长度
		commandLen, err = BytesToInt(data[1:])
		if err != nil {
			return "", errors.Wrap(err, ErrDataLen)
		}
	}
	command, err := a.getLen(commandLen + 2)
	if err != nil {
		return "", err
	}
	if len(command) < 2 {
		return "", errors.New(ErrCommandLenFormat)
	}
	return string(command[:len(command)-2]), nil
}

// 获取一行
func (a *AofParser) getLine() ([]byte, error) {
	data, err := a.reader.ReadBytes(Return)
	if err != nil {
		return nil, err
	}
	dataLen := len(data)
	a.offset += int64(dataLen)
	if dataLen < 2 {
		return nil, errors.New(ErrAofLineLengthFormat)
	}
	if data[dataLen-2] == Carriage && data[dataLen-1] == Return { // 最后一位是\r
		return data[:dataLen-2], nil
	}

	return nil, errors.New(ErrAofLineEndFormat)
}

// 获取指定长度
func (a *AofParser) getLen(dataLen int) ([]byte, error) {
	var p = make([]byte, dataLen)
	size, err := io.ReadFull(a.reader, p)
	a.offset += int64(size)
	return p[:size], err
}

func IntToBytes(n int) []byte {
	data := int64(n)
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, data)
	return bytebuf.Bytes()
}

func BytesToInt(bys []byte) (int, error) {
	return strconv.Atoi(string(bys))
}
