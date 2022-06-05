package io

import (
	"net"
	"time"
)

type Conn struct {
	d    time.Duration
	conn net.Conn
}

type Writer struct {
	Conn
}

type Reader struct {
	Conn
}

func NewWriter(conn net.Conn, deadline time.Duration) *Writer {
	return &Writer{
		Conn{
			d:    deadline,
			conn: conn,
		},
	}
}

func (r *Writer) Write(b []byte) (int, error) {
	if err := r.conn.SetWriteDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.conn.Write(b)
}

func NewReader(conn net.Conn, deadline time.Duration) *Reader {
	return &Reader{
		Conn{
			d:    deadline,
			conn: conn,
		},
	}
}

func (r *Reader) Read(b []byte) (int, error) {
	if err := r.conn.SetReadDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.conn.Read(b)
}

type ReadWriteCloser struct {
	*Writer
	*Reader
	*Closer
}

func NewRWCloser(conn net.Conn, deadline time.Duration) *ReadWriteCloser {
	return &ReadWriteCloser{
		Writer: NewWriter(conn, deadline),
		Reader: NewReader(conn, deadline),
		Closer: &Closer{conn: conn},
	}
}

type Closer struct {
	conn net.Conn
}

func (c *Closer) Close() error {
	return c.conn.Close()
}
