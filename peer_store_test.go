package dht

import (
	"testing"
)

func TestPeerStorage(t *testing.T) {
	ih, err := DecodeInfoHash("d1c5676ae7ac98e8b19f63565905105e3c4c37a2")
	if err != nil {
		t.Fatalf("DecodeInfoHash: %v", err)
	}
	// Allow 1 IH and 2 peers.
	peerStore := newPeerStore(1, 2)

	peer := &peerInfo{addr: "abcd", via: "", cost: 1}
	if added := peerStore.updateContact(ih, peer); !added {
		t.Fatalf("add abcd failed")
	}
	if peerStore.count(ih) != 1 {
		t.Fatalf("Added 1st contact, got count %v, wanted 1", peerStore.count(ih))
	}
	peer = &peerInfo{addr: "efgh", via: "", cost: 1}
	if added := peerStore.updateContact(ih, peer); !added {
		t.Fatalf("add efgh failed")
	}
	if peerStore.count(ih) != 2 {
		t.Fatalf("Added 2st contact, got count %v, wanted 2", peerStore.count(ih))
	}

	peer = &peerInfo{addr: "xyz", via: "", cost: 1}
	if added := peerStore.updateContact(ih, peer); added {
		t.Fatalf("xyz is added")
	}
	if peerStore.count(ih) != 2 {
		t.Fatalf("Added 3st contact, got count %v, wanted 2", peerStore.count(ih))
	}

	peer = &peerInfo{addr: "xyz1", via: "", cost: 0}
	if added := peerStore.updateContact(ih, peer); !added {
		t.Fatalf("xyz1 is not added")
	}
	if peerStore.count(ih) != 2 {
		t.Fatalf("xyz1 got count %v, wanted 2", peerStore.count(ih))
	}
}
