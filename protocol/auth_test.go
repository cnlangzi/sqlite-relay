package protocol

import (
	"os"
	"testing"
)

func TestTokenIsReadForEveryHandshake(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	addr, _, _ := startSource(t, cfg, src)

	old := dialClient(t, addr)
	old.write(testHello(t, cfg, testToken))
	typ, _, err := old.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgHelloAck {
		t.Fatalf("the first message is %s, want HELLO_ACK", typ)
	}

	rotated := "the token of the next rotation"
	if err := os.WriteFile(cfg.TokenFile, []byte(rotated), 0o600); err != nil {
		t.Fatalf("write the token: %v", err)
	}

	// The connection that is open keeps the token it proved itself with.
	old.write(Heartbeat{UnixMillis: 1})
	typ, _, err = old.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgHeartbeat {
		t.Errorf("the source answered %s, want HEARTBEAT", typ)
	}
	old.conn.Close()

	// A new connection has to prove itself with the new token.
	fresh := dialClient(t, addr)
	fresh.write(testHello(t, cfg, rotated))
	typ, _, err = fresh.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgHelloAck {
		t.Fatalf("the first message is %s, want HELLO_ACK", typ)
	}
	fresh.conn.Close()

	// The old token does not open a new connection any more.
	stale := dialClient(t, addr)
	stale.write(testHello(t, cfg, testToken))
	typ, _, err = stale.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgError {
		t.Fatalf("the first message is %s, want ERROR", typ)
	}
}

func TestSourceRejectsATamperedHello(t *testing.T) {
	cfg := testConfig(t)
	src := &fakeSource{}
	addr, _, _ := startSource(t, cfg, src)

	// The code of the hello covers the bytes that come before it, a hello
	// that was changed after it was signed does not match any more.
	hello := testHello(t, cfg, testToken)
	hello.AppliedSeq = 5
	cl := dialClient(t, addr)
	cl.write(hello)
	typ, payload, err := cl.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != MsgError {
		t.Fatalf("the first message is %s, want ERROR", typ)
	}
	m, err := decodeError(payload)
	if err != nil {
		t.Fatalf("decodeError: %v", err)
	}
	if m.Code != CodeAuth {
		t.Errorf("the code is %s, want %s", m.Code, CodeAuth)
	}
}

func TestLoadToken(t *testing.T) {
	path := t.TempDir() + "/token"
	if err := os.WriteFile(path, []byte("  the token\n"), 0o600); err != nil {
		t.Fatalf("write the token: %v", err)
	}
	token, err := LoadToken(path)
	if err != nil {
		t.Fatalf("LoadToken: %v", err)
	}
	if string(token) != "the token" {
		t.Errorf("the token is %q, want %q", token, "the token")
	}
	if _, err := LoadToken(path + ".missing"); err == nil {
		t.Error("a missing token file was accepted")
	}
	empty := t.TempDir() + "/empty"
	if err := os.WriteFile(empty, []byte("  \n"), 0o600); err != nil {
		t.Fatalf("write the token: %v", err)
	}
	if _, err := LoadToken(empty); err == nil {
		t.Error("an empty token file was accepted")
	}
}

func TestMACNoticesAChangedByte(t *testing.T) {
	nonce, err := newNonce()
	if err != nil {
		t.Fatalf("newNonce: %v", err)
	}
	token := []byte(testToken)
	prefix := []byte("the bytes of the hello")
	mac := clientMAC(token, nonce, prefix)
	if !macEqual(mac, clientMAC(token, nonce, prefix)) {
		t.Fatal("the code of the hello does not match itself")
	}
	changed := append([]byte(nil), prefix...)
	changed[0] ^= 0xff
	if macEqual(mac, clientMAC(token, nonce, changed)) {
		t.Error("the code of the hello did not notice the changed byte")
	}
	other, err := newNonce()
	if err != nil {
		t.Fatalf("newNonce: %v", err)
	}
	if macEqual(mac, clientMAC(token, other, prefix)) {
		t.Error("the code of the hello did not notice the changed nonce")
	}
	if macEqual(mac, clientMAC([]byte("another token"), nonce, prefix)) {
		t.Error("the code of the hello did not notice the changed token")
	}
}

func TestServerMACNoticesAChangedByte(t *testing.T) {
	client, err := newNonce()
	if err != nil {
		t.Fatalf("newNonce: %v", err)
	}
	server, err := newNonce()
	if err != nil {
		t.Fatalf("newNonce: %v", err)
	}
	token := []byte(testToken)
	prefix := []byte("the bytes of the answer")
	mac := serverMAC(token, client, server, prefix)
	if !macEqual(mac, serverMAC(token, client, server, prefix)) {
		t.Fatal("the code of the answer does not match itself")
	}
	changed := append([]byte(nil), prefix...)
	changed[0] ^= 0xff
	if macEqual(mac, serverMAC(token, client, server, changed)) {
		t.Error("the code of the answer did not notice the changed byte")
	}
	if macEqual(mac, serverMAC(token, server, client, prefix)) {
		t.Error("the code of the answer did not notice the swapped nonce")
	}
}

func TestNewNonce(t *testing.T) {
	a, err := newNonce()
	if err != nil {
		t.Fatalf("newNonce: %v", err)
	}
	b, err := newNonce()
	if err != nil {
		t.Fatalf("newNonce: %v", err)
	}
	if a == b {
		t.Error("two nonces are the same")
	}
}
