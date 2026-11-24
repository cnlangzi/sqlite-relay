package protocol

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
)

// The labels that separate the two challenges of the handshake.
const (
	backupHelloLabel = "backup-hello"
	sourceHelloLabel = "source-hello"
)

// LoadToken reads the shared token from the file at path.
//
// The file is read again for every handshake, so that a token that was
// rotated takes effect without a restart. The white space around the token
// is removed.
func LoadToken(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("protocol: read the token: %w", err)
	}
	token := bytes.TrimSpace(b)
	if len(token) == 0 {
		return nil, errors.New("protocol: the token file is empty")
	}
	return token, nil
}

// clientMAC computes the message authentication code of a hello.
//
// The code covers the label of the challenge, the nonce of the backup and
// the bytes of the hello that come before the code itself.
func clientMAC(token []byte, nonce [nonceSize]byte, prefix []byte) [macSize]byte {
	h := hmac.New(sha256.New, token)
	h.Write([]byte(backupHelloLabel))
	h.Write(nonce[:])
	h.Write(prefix)
	var mac [macSize]byte
	copy(mac[:], h.Sum(nil))
	return mac
}

// serverMAC computes the message authentication code of a hello answer.
//
// The code covers the label of the challenge, the nonce of the backup, the
// nonce of the source and the bytes of the answer that come before the code
// itself.
func serverMAC(token []byte, clientNonce, serverNonce [nonceSize]byte, prefix []byte) [macSize]byte {
	h := hmac.New(sha256.New, token)
	h.Write([]byte(sourceHelloLabel))
	h.Write(clientNonce[:])
	h.Write(serverNonce[:])
	h.Write(prefix)
	var mac [macSize]byte
	copy(mac[:], h.Sum(nil))
	return mac
}

// macEqual compares two message authentication codes in constant time.
func macEqual(a, b [macSize]byte) bool {
	return hmac.Equal(a[:], b[:])
}

// newNonce returns a fresh nonce.
func newNonce() ([nonceSize]byte, error) {
	var nonce [nonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nonce, fmt.Errorf("protocol: read the nonce: %w", err)
	}
	return nonce, nil
}
