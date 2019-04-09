package dht

import (
	"container/ring"

	"github.com/golang/groupcache/lru"
)

type peerInfoWire struct {
	Addr	[]byte
	Cost	uint32
}

type peerInfo struct {
	addr	string
	via		string
	cost	uint32
	alive	bool
}

// For the inner map, the key address in binary form. value=ignored.
type peerContactsSet struct {
	set			map[string]*peerInfo
	indirects	map[string]*peerInfo // peers cost > 0
	// Needed to ensure different peers are returned each time.
	ring		*ring.Ring
}

// next returns up to 8 peer contacts, if available. Further calls will return a
// different set of contacts, if possible.
func (p *peerContactsSet) next() []*peerInfo {
	count := kNodes
	if count > len(p.set) {
		count = len(p.set)
	}
	var deleted []*peerInfo
	var ret []*peerInfo
	for range p.set {
		v := p.ring.Next().Value.(*peerInfo)
		if !v.alive {
			// delete died peer
			p.ring.Unlink(1)
			delete(p.set, v.addr)
			delete(p.indirects, v.addr)

			deleted = append(deleted, v)
			continue
		}
		p.ring = p.ring.Next()
		ret = append(ret, v)
		if len(ret) >= count {
			break
		}
	}

	if len(ret) < count {
		for _, v := range deleted {
			ret = append(ret, v)
			if len(ret) >= count {
				break
			}
		}
	}
	return ret
}

// put adds a peerContact to an infohash contacts set. peerContact must be a binary encoded contact
// address where the first four bytes form the IP and the last byte is the port. IPv6 addresses are
// not currently supported. peerContact with less than 6 bytes will not be stored.
func (p *peerContactsSet) put(peerContact *peerInfo, removeIndirect bool) bool {
	if v, ok := p.set[peerContact.addr]; ok {
		if v.cost <= peerContact.cost && v.alive {
			return false
		}
		// same target peer, but lower cost route
		v.cost = peerContact.cost
		v.via = peerContact.via
		v.alive = peerContact.alive
		if v.cost == 0 {
			delete(p.indirects, v.addr)
		}
		return true
	}
	if removeIndirect {
		if len(p.indirects) == 0 {
			return false
		}
		for _, v := range p.indirects {
			if v.cost <= peerContact.cost {
				continue
			}
			delete(p.indirects, v.addr)
			delete(p.set, v.addr)
			// replace member variables so that we don't need to deal with the ring
			v.addr = peerContact.addr
			v.cost = peerContact.cost
			v.via = peerContact.via
			v.alive = peerContact.alive
			p.set[v.addr] = v
			if v.cost > 0 {
				p.indirects[v.addr] = v
			}
			return true
		}
		return false
	}
	p.set[peerContact.addr] = peerContact
	if peerContact.cost > 0 {
		p.indirects[peerContact.addr] = peerContact
	}
	r := &ring.Ring{Value: peerContact}
	if p.ring == nil {
		p.ring = r
	} else {
		p.ring.Link(r)
	}
	return true
}

func (p *peerContactsSet) kill(peerContact string) {
	if _, ok := p.set[peerContact]; ok {
		p.set[peerContact].alive = false
	}
}

// Size is the number of contacts known for an infohash.
func (p *peerContactsSet) Size() int {
	return len(p.set)
}

func (p *peerContactsSet) Alive() int {
	var ret int = 0
	for ih := range p.set {
		if p.set[ih].alive {
			ret++
		}
	}
	return ret
}

func newPeerStore(maxInfoHashes, maxInfoHashPeers int) *peerStore {
	return &peerStore{
		infoHashPeers:        lru.New(maxInfoHashes),
		localActiveDownloads: make(map[InfoHash]int),
		maxInfoHashes:        maxInfoHashes,
		maxInfoHashPeers:     maxInfoHashPeers,
	}
}

type peerStore struct {
	// cache of peers for infohashes. Each key is an infohash and the
	// values are peerContactsSet.
	infoHashPeers *lru.Cache
	// infoHashes for which we are peers.
	localActiveDownloads map[InfoHash]int // value is port number
	maxInfoHashes        int
	maxInfoHashPeers     int
}

func (h *peerStore) get(ih InfoHash) *peerContactsSet {
	c, ok := h.infoHashPeers.Get(string(ih))
	if !ok {
		return nil
	}
	contacts := c.(*peerContactsSet)
	return contacts
}

// count shows the number of known peers for the given infohash.
func (h *peerStore) count(ih InfoHash) int {
	peers := h.get(ih)
	if peers == nil {
		return 0
	}
	return peers.Size()
}

func (h *peerStore) alive(ih InfoHash) int {
	peers := h.get(ih)
	if peers == nil {
		return 0
	}
	return peers.Alive()
}

// peerContacts returns a random set of 8 peers for the ih InfoHash.
func (h *peerStore) peerContacts(ih InfoHash) []*peerInfo {
	peers := h.get(ih)
	if peers == nil {
		return nil
	}
	return peers.next()
}

// addContact as a peer for the provided ih. Returns true if the contact was
// added, false otherwise (e.g: already present, or invalid).
func (h *peerStore) updateContact(ih InfoHash, peerContact *peerInfo) bool {
	peerContact.alive = true
	var peers *peerContactsSet
	p, ok := h.infoHashPeers.Get(string(ih))
	if !ok {
		peers = &peerContactsSet{set: make(map[string]*peerInfo), indirects: make(map[string]*peerInfo)}
		h.infoHashPeers.Add(string(ih), peers)
		return peers.put(peerContact, false)
	}

	var okType bool
	if peers, okType = p.(*peerContactsSet); !okType { // should never happend
		peers = &peerContactsSet{set: make(map[string]*peerInfo), indirects: make(map[string]*peerInfo)}
		h.infoHashPeers.Add(string(ih), peers)
		return peers.put(peerContact, false)
	}
	/*
	if peers.Size() >= h.maxInfoHashPeers {
		// max peer limition exceeded
		return peers.put(peerContact, true)
	}
	*/
	// h.infoHashPeers.Add(string(ih), peers)
	return peers.put(peerContact, peers.Size() >= h.maxInfoHashPeers)
}

func (h *peerStore) killContact(peerContact string) {
	if h == nil {
		return
	}
	for ih := range h.localActiveDownloads {
		if p := h.get(ih); p != nil {
			p.kill(peerContact)
		}
	}
}

func (h *peerStore) addLocalDownload(ih InfoHash, port int) {
	h.localActiveDownloads[ih] = port
}

func (h *peerStore) hasLocalDownload(ih InfoHash) (port int) {
	port, _ = h.localActiveDownloads[ih]
	return
}
