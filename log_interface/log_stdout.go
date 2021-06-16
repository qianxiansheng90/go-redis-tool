/*
 *Descript:日志输出到stdout
 */
package log_interface

import "fmt"

type LogStdout struct{}

func NewLogStdout() Logger {
	return &LogStdout{}
}

func (l *LogStdout) Debug(v ...interface{}) {
	fmt.Println(v...)
}
func (l *LogStdout) Debugf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}
func (l *LogStdout) Info(v ...interface{}) {
	fmt.Println(v...)
}
func (l *LogStdout) Infof(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}
func (l *LogStdout) Warn(v ...interface{}) {
	fmt.Println(v...)
}
func (l *LogStdout) Warnf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}
func (l *LogStdout) Error(v ...interface{}) {
	fmt.Println(v...)
}
func (l *LogStdout) Errorf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}
func (l *LogStdout) Fatal(v ...interface{}) {
	fmt.Println(v...)
}
func (l *LogStdout) Fatalf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}
func (l *LogStdout) Panic(v ...interface{}) {
	fmt.Println(v...)
}
func (l *LogStdout) Panicf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}
