package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn    io.ReadWriteCloser
	buf     *bufio.Writer
	encoder *gob.Encoder
	decoder *gob.Decoder
}

var _ Codec = (*GobCodec)(nil)

func newGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn:    conn,
		buf:     buf,
		decoder: gob.NewDecoder(conn),
		encoder: gob.NewEncoder(buf),
	}
}

func (c *GobCodec) readHeader(h *header) error {
	return c.decoder.Decode(h)
}
func (c *GobCodec) readBody(body interface{}) error {
	return c.decoder.Decode(body)
}
func (c *GobCodec) Close() error {
	return c.conn.Close()
}
func (c *GobCodec) write(h *header, body interface{}) (err error) {
	defer func() {
		c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	if err := c.encoder.Encode(h); err != nil {
		log.Println("rpc: gob error encoding header:", err)
		return
	}
	if err := c.encoder.Encode(body); err != nil {
		log.Println("rpc:gob error encoding body:", err)
		return
	}
	return
}
