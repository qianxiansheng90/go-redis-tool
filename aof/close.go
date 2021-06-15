package aof
/*
 *Descript:关闭aof解析器
 */
// 关闭
func (a *AofParser) Close() error {
	if a.file != nil {
		return a.file.Close()
	}
	return nil
}
