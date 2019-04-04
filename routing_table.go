package dht

import (
	"expvar"
	"fmt"
	"net"
	"time"

	"github.com/nictuku/nettools"
)

func newRoutingTable(log *DebugLogger) *routingTable {
	return &routingTable{
		nTree:     &nTree{},
		addresses: make(map[string]*remoteNode),
		hotspots:  make(map[string]*hotspot),
		hsBoundary: make(map[string]*hotspot),
		hsMgnted: make(map[string]*hotspot),
		log:       log,
	}
}

type hotspot struct {
	id			string
	typ 		int		// 1 for node, 2 for data
	proximity	int
	distance	uint32

	boundary	*remoteNode
	bProximity	int
	bDistance	uint32
}

type routingTable struct {
	*nTree
	// addresses is a map of UDP addresses in host:port format and
	// remoteNodes. A string is used because it's not possible to create
	// a map using net.UDPAddr
	// as a key.
	addresses		map[string]*remoteNode

	// Neighborhood.
	nodeId       	string // This shouldn't be here. Move neighborhood upkeep one level up?
	//boundaryNode 	*remoteNode
	// How many prefix bits are shared between boundaryNode and nodeId.
	//proximity 		int

	// hotspots
	hotspots		map[string]*hotspot  // InfoHash to hotspot pair
	hsBoundary		map[string]*hotspot  // Boundary to hotspot pair for o(1) search
	hsMgnted		map[string]*hotspot	 // InfoHash to hotspot pair for those should mgnted by this node

	log *DebugLogger
}

func (r *routingTable) addHotspot(id string, typ int) {
	r.hotspots[id] = &hotspot{id: id, typ: typ}
}

// hostPortToNode finds a node based on the specified hostPort specification,
// which should be a UDP address in the form "host:port".
func (r *routingTable) hostPortToNode(hostPort string, port string) (node *remoteNode, addr string, existed bool, err error) {
	if hostPort == "" {
		panic("programming error: hostPortToNode received a nil hostPort")
	}
	address, err := net.ResolveUDPAddr(port, hostPort)
	if err != nil {
		return nil, "", false, err
	}
	if address.String() == "" {
		return nil, "", false, fmt.Errorf("programming error: address resolution for hostPortToNode returned an empty string")
	}
	(*r.log).Debugf("hostPortToNode key: %s", address.String())
	n, existed := r.addresses[address.String()]
	if existed && n == nil {
		return nil, "", false, fmt.Errorf("programming error: hostPortToNode found nil node in address table")
	}
	return n, address.String(), existed, nil
}

func (r *routingTable) length() int {
	return len(r.addresses)
}

func (r *routingTable) reachableNodes() (tbl map[string][]byte) {
	tbl = make(map[string][]byte)
	for addr, r := range r.addresses {
		if addr == "" {
			(*r.log).Debugf("reachableNodes: found empty address for node %x.", r.id)
			continue
		}
		if r.reachable && len(r.id) == 20 {
			tbl[addr] = []byte(r.id)
		}
	}

	// hexId := fmt.Sprintf("%x", r.nodeId)
	// This creates a new expvar everytime, but the alternative is too
	// bothersome (get the current value, type cast it, ensure it
	// exists..). Also I'm not using NewInt because I don't want to publish
	// the value.
	v := new(expvar.Int)
	v.Set(int64(len(tbl)))
	// reachableNodes.Set(hexId, v)
	return

}

// update the existing routingTable entry for this node by setting its correct
// infohash id. Gives an error if the node was not found.
func (r *routingTable) update(node *remoteNode, proto string) error {
	_, addr, existed, err := r.hostPortToNode(node.address.String(), proto)
	if err != nil {
		return err
	}
	if !isValidAddr(addr) {
		return fmt.Errorf("routingTable.update received an invalid address %v", addr)
	}
	if !existed {
		return fmt.Errorf("node missing from the routing table: %v", node.address.String())
	}
	if node.id != "" {
		r.nTree.insert(node)
		totalNodes.Add(1)
		r.addresses[addr].id = node.id
	}
	return nil
}

// insert the provided node into the routing table. Gives an error if another
// node already existed with that address.
func (r *routingTable) insert(node *remoteNode, proto string) error {
	if node.address.Port == 0 {
		return fmt.Errorf("routingTable.insert() got a node with Port=0")
	}
	if node.address.IP.IsUnspecified() {
		return fmt.Errorf("routingTable.insert() got a node with a non-specified IP address")
	}
	_, addr, existed, err := r.hostPortToNode(node.address.String(), proto)
	if err != nil {
		return err
	}
	if !isValidAddr(addr) {
		return fmt.Errorf("routingTable.insert received an invalid address %v", addr)

	}
	if existed {
		return nil // fmt.Errorf("node already existed in routing table: %v", node.address.String())
	}
	r.addresses[addr] = node
	// We don't know the ID of all nodes.
	if !bogusId(node.id) {
		// recursive version of node insertion.
		r.nTree.insert(node)
		totalNodes.Add(1)
	}
	return nil
}

// getOrCreateNode returns a node for hostPort, which can be an IP:port or
// Host:port, which will be resolved if possible.  Preferably return an entry
// that is already in the routing table, but create a new one otherwise, thus
// being idempotent.
func (r *routingTable) getOrCreateNode(id string, hostPort string, proto string) (node *remoteNode, err error) {
	node, addr, existed, err := r.hostPortToNode(hostPort, proto)
	if err != nil {
		return nil, err
	}
	if existed {
		return node, nil
	}
	udpAddr, err := net.ResolveUDPAddr(proto, addr)
	if err != nil {
		return nil, err
	}
	node = newRemoteNode(*udpAddr, id, r.log)
	(*r.log).Debugf("new node: %p", node)
	return node, r.insert(node, proto)
}

func (r *routingTable) kill(n *remoteNode, p *peerStore) {
	(*r.log).Debugf("kill node: %p", n)
	delete(r.addresses, n.address.String())
	r.nTree.cut(InfoHash(n.id), 0)
	totalKilledNodes.Add(1)

	//if r.boundaryNode != nil && n.id == r.boundaryNode.id {
	if hs, ok := r.hsBoundary[n.id]; ok {
		r.resetNeighborhoodBoundary(hs)
	}
	p.killContact(nettools.BinaryToDottedPort(n.addressBinaryFormat))
}

func (r *routingTable) resetNeighborhoodBoundary(hs *hotspot) {
	hs.bProximity = 0
	hs.bDistance = 0
	// Try to find a distant one within the neighborhood and promote it as
	// the most distant node in the neighborhood.
	neighbors := r.lookup(InfoHash(hs.id))
	if len(neighbors) > 0 {
		if hs.boundary != nil {
			if _, ok := r.hsBoundary[hs.boundary.id]; ok {
				delete(r.hsBoundary, hs.boundary.id)
			}
		}

		hs.boundary = neighbors[len(neighbors)-1]
		hs.bProximity = commonBits(hs.id, hs.boundary.id)
		hs.bDistance = calcDistance(hs.id, hs.boundary.id)
		r.hsBoundary[hs.boundary.id] = hs

		proximity := commonBits(hs.id, r.nodeId)
		distance := calcDistance(hs.id, r.nodeId)
		_, isMgnted := r.hsMgnted[hs.id]
		if isMgnted && (proximity < hs.bProximity || distance > hs.bDistance) {
			(*r.log).Debugf("[Duat] hotspot %x is NOT managed by me any more", hs.id)
			delete(r.hsMgnted, hs.id)
		}
		if proximity >= hs.bProximity || distance <= hs.bDistance {
			(*r.log).Debugf("[Duat] hotspot %x is managed by me", hs.id)
			r.hsMgnted[hs.id] = hs
		} else {
			(*r.log).Debugf("[Duat] hotspot %x is not my biz", hs.id)
		}
	}
}

func (r *routingTable) cleanup(cleanupPeriod time.Duration, p *peerStore) (needPing []*remoteNode) {
	needPing = make([]*remoteNode, 0, 10)
	t0 := time.Now()
	// Needs some serious optimization.
	for addr, n := range r.addresses {
		if addr != n.address.String() {
			(*r.log).Debugf("cleanup: node address mismatches: %v != %v. Deleting node", addr, n.address.String())
			r.kill(n, p)
			continue
		}
		if addr == "" {
			(*r.log).Debugf("cleanup: found empty address for node %x. Deleting node", n.id)
			r.kill(n, p)
			continue
		}
		if n.reachable {
			if len(n.pendingQueries) == 0 {
				goto PING
			}
			// Tolerate 2 cleanup cycles.
			if time.Since(n.lastResponseTime) > cleanupPeriod*2+(cleanupPeriod/15) {
				(*r.log).Debugf("DHT: Old node seen %v ago. Deleting", time.Since(n.lastResponseTime))
				r.kill(n, p)
				continue
			}
			if time.Since(n.lastResponseTime).Nanoseconds() < cleanupPeriod.Nanoseconds()/2 {
				// Seen recently. Don't need to ping.
				continue
			}

		} else {
			// Not reachable.
			if len(n.pendingQueries) > maxNodePendingQueries {
				// Didn't reply to 2 consecutive queries.
				(*r.log).Debugf("DHT: Node never replied to ping. Deleting. %v", n.address)
				r.kill(n, p)
				continue
			}
		}
	PING:
		needPing = append(needPing, n)
	}
	duration := time.Since(t0)
	// If this pauses the server for too long I may have to segment the cleanup.
	// 2000 nodes: it takes ~12ms
	// 4000 nodes: ~24ms.
	(*r.log).Debugf("DHT: Routing table cleanup took %v\n", duration)
	return needPing
}

// neighborhoodUpkeep will update the routingtable if the node n is closer than
// the 8 nodes in our neighborhood, by replacing the least close one
// (boundary). n.id is assumed to have length 20.
func (r *routingTable) neighborhoodUpkeep(hsId string, n *remoteNode, proto string, p *peerStore) (bool, error) {
	(*r.log).Debugf("[Duat] neighborhoodUpkeep the hotspot %x", hsId)
	hs, ok := r.hotspots[hsId]
	if !ok {
		return false, fmt.Errorf("no such hotspot")
	}
	if hs.boundary == nil {
		r.addNewNeighbor(hs, n, false, proto, p)
		return true, nil
	}
	if r.length() < (len(r.hotspots) * kNodes) {
		r.addNewNeighbor(hs, n, false, proto, p)
		return true, nil
	}
	cmp := commonBits(hsId, n.id)
	if cmp > hs.bProximity {
		r.addNewNeighbor(hs, n, true, proto, p)
		return true, nil
	}
	if cmp == 0 {
		(*r.log).Debugf("[Duat] hs %x and node %x commonbits is 0", hsId, n.id)
		distance := calcDistance(hsId, n.id)
		if distance < hs.bDistance {
			r.addNewNeighbor(hs, n, true, proto, p)
			return true, nil
		}
		// Not significantly better.
		return false, nil
	}
	return false, nil
}

func (r *routingTable) addNewNeighbor(hs *hotspot, n *remoteNode, displaceBoundary bool, proto string, p *peerStore) {
	if err := r.insert(n, proto); err != nil {
		(*r.log).Debugf("addNewNeighbor error: %v", err)
		return
	}
	/*
	if displaceBoundary && hs.boundary != nil {
		// This will also take care of setting a new boundary.
		r.kill(hs.boundary, p)
	} else {
		r.resetNeighborhoodBoundary(hs)
	}*/
	r.resetNeighborhoodBoundary(hs)
	(*r.log).Debugf("New neighbor added %s with proximity %d, distance %d", nettools.BinaryToDottedPort(n.addressBinaryFormat), hs.bProximity, hs.bDistance)
}

// pingSlowly pings the remote nodes in needPing, distributing the pings
// throughout an interval of cleanupPeriod, to avoid network traffic bursts. It
// doesn't really send the pings, but signals to the main goroutine that it
// should ping the nodes, using the pingRequest channel.
func pingSlowly(pingRequest chan *remoteNode, needPing []*remoteNode, cleanupPeriod time.Duration, stop chan bool) {
	if len(needPing) == 0 {
		return
	}
	duration := cleanupPeriod - (1 * time.Minute)
	perPingWait := duration / time.Duration(len(needPing))
	for _, r := range needPing {
		pingRequest <- r
		select {
		case <-time.After(perPingWait):
		case <-stop:
			return
		}
	}
}

func isValidAddr(addr string) bool {
	if addr == "" {
		return false
	}
	if h, p, err := net.SplitHostPort(addr); h == "" || p == "" || err != nil {
		return false
	}
	return true
}

var (
	// totalKilledNodes is a monotonically increasing counter of times nodes were killed from
	// the routing table. If a node is later added to the routing table and killed again, it is
	// counted twice.
	totalKilledNodes = expvar.NewInt("totalKilledNodes")
	// totalNodes is a monotonically increasing counter of times nodes were added to the routing
	// table. If a node is removed then later added again, it is counted twice.
	totalNodes = expvar.NewInt("totalNodes")
	// reachableNodes is the count of all reachable nodes from a particular DHT node. The map
	// key is the local node's infohash. The value is a gauge with the count of reachable nodes
	// at the latest time the routing table was persisted on disk.
	reachableNodes = expvar.NewMap("reachableNodes")
)