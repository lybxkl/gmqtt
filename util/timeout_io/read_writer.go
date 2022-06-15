package io

import (
	"net"
	"time"
)

type Conn struct {
	d time.Duration
	net.Conn
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
			Conn: conn,
		},
	}
}

func (r *Writer) Write(b []byte) (int, error) {
	if err := r.Conn.SetWriteDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.Conn.Write(b)
}

func NewReader(conn net.Conn, deadline time.Duration) *Reader {
	return &Reader{
		Conn{
			d:    deadline,
			Conn: conn,
		},
	}
}

func (r *Reader) Read(b []byte) (int, error) {
	if err := r.Conn.SetReadDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.Conn.Read(b)
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
