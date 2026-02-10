package codec

import (
	"io"
)

type header struct {
	serviceMethod string
	seq           uint64
	err           string
}

type Codec interface {
	io.Closer
	readHeader(*header) error
	readBody(interface{}) error
	write(*header, interface{}) error
}
type newCodecFunc func(closer io.ReadWriteCloser) Codec

type Type string

var newCodecFuncMap map[Type]newCodecFunc

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json" // not implemented
)

func init() {
	newCodecFuncMap = make(map[Type]newCodecFunc)
	newCodecFuncMap[GobType] = newGobCodec
}
