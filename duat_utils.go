package dht

import (
	"fmt"
	"crypto/rand"
	"crypto/sha1"
	"net"
	"io"
	"encoding/binary"
)

func randNodeId() ([]byte, error) {
	b := make([]byte, 20)
	_, err := io.ReadFull(rand.Reader, b)
	return b, err
}

func newTokenSecret() (string, error) {
	b := make([]byte, 5)
	_, err := rand.Read(b)
	return string(b), err
}

func genToken(addr net.UDPAddr, secret string) string {
	h := sha1.New()
	io.WriteString(h, addr.String())
	io.WriteString(h, secret)
	return fmt.Sprintf("%x", h.Sum(nil))
}
func checkToken(secrets []string, addr net.UDPAddr, token string) bool {
	for _, secret := range secrets {
		if genToken(addr, secret) == token {
			return true
		}
	}
	return false
}

func calcDistance(id1 string, id2 string) uint32 {
		/*
		d := make([]byte, len(id1))
		if len(id1) != len(id2) {
			return ""
		} else {
			for i := 0; i < len(id1); i++ {
				d[i] = id1[i] ^ id2[i]
			}
			return string(d)
		}
		return ""
		*/
		/*
		d := make([]byte, len(id1))
		for i := 0; i < len(id1); i++ {
			d[i] = id1[i] ^ id2[i]
		}
		ret := binary.LittleEndian.Uint32(d)
		*/
		d1 := binary.LittleEndian.Uint32([]byte(id1))
		d2 := binary.LittleEndian.Uint32([]byte(id2))
		return d1 ^ d2
}