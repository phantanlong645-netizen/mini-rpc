package codec

import (
	"io"
)

type Header struct {
	ServiceMethod string
	Seq           uint64
	Err           string
}

type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}
type newCodecFunc func(closer io.ReadWriteCloser) Codec

type Type string

var NewCodecFuncMap map[Type]newCodecFunc

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json" // not implemented
)

func init() {
	NewCodecFuncMap = make(map[Type]newCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
