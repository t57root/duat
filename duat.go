package dht

import (
	//"fmt"
	"net"
	"time"
	"strings"
	"sync"
	"github.com/nictuku/nettools"
)

var MinNodes = 10
var MaxNodes = 100
var MaxInfoHashes = 2048
var MaxPeersPerInfoHash = 256
var SaveRoutingTable = true

var ClientPerMinuteLimit = 50
var ThrottlerTrackedClients = int64(1000)
var RateLimit = -1 // -1 to disable limition

var MaxUDPPacketSize = 4096

// var RoutingTableCleanupPeriod = 15 * time.Minute
var RoutingTableCleanupPeriod = 10 * time.Second
var SecretRotatePeriod = 5 * time.Minute
var DuatStorePeriod = 5 * time.Minute

type ihReq struct {
	target  *remoteNode
	ih      InfoHash
	options announceOptions
}

type announceOptions struct {
	announce bool
	port     int
}

type Duat struct {
	NodeId			InfoHash
	NodeIdStr		string

	outable			bool
	////// core data structs //////
	routingTable	*routingTable
	peerStore		*peerStore
	duatStore		*dhtStore
	////// utils //////
	listener		*net.UDPConn
	throttler		*nettools.ClientThrottle
	log				DebugLogger
	////// channels for async ops //////
	stopChan		chan bool
	 //reqAddNode		chan string
	reqPingChan		chan ihReq
	reqFindNodeChan	chan ihReq
	reqGetPeersChan	chan ihReq
	 //reqAnnounce		chan bool
	////// runtime data //////
	netAddr			string
	netPort			int
	netProto		string
	wg				sync.WaitGroup
	bytesArena		arena
	packetChan		chan packetType
	tokenSecrets    []string
	////// config //////
	seed			[]string
}

func NewDuat (port int, seed []string) (duat *Duat, err error) {
	var logger DebugLogger
	logger = (&printLogger{})

	if seed == nil {
		seed = []string{}
	}

	duatStore := openStore(port, SaveRoutingTable)
	if len(duatStore.Id) != 20 {
		var err error
		duatStore.Id, err = randNodeId()
		if err != nil { return nil, err }
		logger.Infof("[Duat] using a new random node id: %x", duatStore.Id)
		saveStore(*duatStore)
	}

	routingTable := newRoutingTable(&logger)
	routingTable.nodeId = string(duatStore.Id)
	routingTable.addHotspot(string(duatStore.Id), 1)
	xxxih, xxxe := DecodeInfoHash("deca7a89a1dbdc4b213de1c0d5351e92582f31fb")
    if xxxe != nil {
        return nil, xxxe
	}
	routingTable.addHotspot(string(xxxih), 2)

	tokenSecret1, err1 := newTokenSecret()
	tokenSecret2, err2 := newTokenSecret()
	if err1 != nil || err2 != nil {
		logger.Errorf("newTokenSecret failed: err1: %s, err2: %s", err1, err2)
	}

	return &Duat{
		NodeId:			InfoHash(duatStore.Id),
		NodeIdStr:		string(duatStore.Id),
		outable:		false,
		// core data structs
		routingTable:	routingTable,
		peerStore:		newPeerStore(MaxInfoHashes, MaxPeersPerInfoHash),
		duatStore:		duatStore,
		// utils
		 //listener:		nil,
		throttler:		nettools.NewThrottler(ClientPerMinuteLimit, ThrottlerTrackedClients),
		log:			logger,
		// channels for async ops 
		stopChan:		make(chan bool),
		 //reqAddNode:		make(chan string, 10),
		reqPingChan:	make(chan ihReq, 10),
		reqFindNodeChan:make(chan ihReq, 10),
		reqGetPeersChan:make(chan ihReq, 10),
		 //reqAnnounce:	make(chan bool, 10),
		// runtime data
		netAddr:		"0.0.0.0",
		netPort:		port,
		netProto:		"udp4",
		 //bytesArena:		arena,
		 //packetChan:		chan packetType
		tokenSecrets:	[]string{tokenSecret1, tokenSecret2},
		// config
		seed:			seed,
	}, nil
}


func (this *Duat) Stop() {
	close(this.stopChan)
	this.log.Infof("[Duat] wait all routines to end")
	this.wg.Wait()
}

func (this *Duat) bootstrap() {
	this.log.Infof("[Duat] %x bootstrap with seed: %s", string(this.NodeId), strings.Join(this.seed, ","))
	for _, s := range this.seed {
		r, e := this.routingTable.getOrCreateNode("", s, this.netProto)
		if e != nil {
			this.log.Errorf("[Duat] bootstrap getOrCreateNode with %s failed: %s", s, e)
			continue
		}
		this.requestPing(r)
		this.requestFindNode(r, this.NodeId)
	}
	// this.castFindNode(this.NodeId)
	// XXX this.getMorePeers
}

func (this *Duat) Start() error {
	var err error
	this.listener, err = listen(this.netAddr, this.netPort, this.netProto, this.log)
	if err != nil {
		return err
	}
	this.netPort = this.listener.LocalAddr().(*net.UDPAddr).Port

	this.bootstrap()
	go func() {
		this.wg.Add(1)
		defer this.wg.Done()
		this.DispatchPacket()
	}()
	go func() {
		this.wg.Add(1)
		defer this.wg.Done()
		this.loop()
	}()

	return nil
}

func (this *Duat) updateHotspots() {
	for _, hs := range this.routingTable.hotspots {
		if hs.typ == 1 { // node
			this.castFindNode(InfoHash(hs.id))
		} else if hs.typ == 2 { // data
			this.castGetPeers(InfoHash(hs.id))
		}
	}
}

func (this *Duat) DispatchPacket() {
	defer this.listener.Close()

	this.bytesArena = newArena(MaxUDPPacketSize, 3)
	this.packetChan = make(chan packetType, 10)

	readFromSocket(this.listener, this.packetChan, this.bytesArena, this.stopChan, this.log)
}

func (this *Duat) loop() {
	routingTableCleanupTicker := time.Tick(RoutingTableCleanupPeriod)
	secretRotateTicker := time.Tick(SecretRotatePeriod)
	duatStoreTicker := time.Tick(DuatStorePeriod)
	tokenBucketTicker := time.Tick(time.Second / 10)

	tokenBucket := RateLimit
	for {
		select {
		case <- this.stopChan:
			this.log.Infof("[Duat] got stop signal, exiting")
			this.throttler.Stop()
			return
		case p := <-this.packetChan:
			if tokenBucket > 0 || RateLimit < 0 {
				tokenBucket -= 1
				this.processPacket(p)
			} else {
				this.log.Infof("[Duat] dorpping packet due to rate limition")
			}
			this.bytesArena.Push(p.b)
		case req := <-this.reqPingChan:
			this.requestPing(req.target)
		case req := <-this.reqFindNodeChan:
			if req.target == nil {
				this.castFindNode(req.ih)
			} else {
				this.requestFindNode(req.target, req.ih)
			}
		case req := <-this.reqGetPeersChan:
			if req.target == nil {
				this.castGetPeers(req.ih)
			} else {
				this.requestGetPeers(req.target, req.ih)
			}
		case <-tokenBucketTicker:
			if RateLimit < 0 {
				continue
			}
			if tokenBucket < RateLimit {
				tokenBucket += RateLimit / 10
			}
		case <- routingTableCleanupTicker:
			this.log.Infof("[Duat] clean up the routing table")
			needPing := this.routingTable.cleanup(RoutingTableCleanupPeriod, this.peerStore)
			if len(needPing) > 0 && false { // XXX
				go func() {
					this.wg.Add(1)
					defer this.wg.Done()

					duration := RoutingTableCleanupPeriod - (1 * time.Minute)
					perPingWait := duration / time.Duration(len(needPing))
					for _, r := range needPing {
						this.reqPingChan <- ihReq{target: r}
						select {
						case <-time.After(perPingWait):
						case <-this.stopChan:
							return
						}
					}
				}()
			}
			this.updateHotspots()
		case <-secretRotateTicker:
			n, err := newTokenSecret()
			if err != nil {
				this.log.Errorf("rotate token secret failed: %s", err)
				continue
			}
			this.tokenSecrets = []string{n, this.tokenSecrets[0]}
		case <-duatStoreTicker:
			tbl := this.routingTable.reachableNodes()
			if len(tbl) > 5 {
				this.duatStore.Remotes = tbl
				saveStore(*this.duatStore)
			}
		}
	}
}

func (this *Duat) processPacket(p packetType) {
	/* XXX
	if !this.throttler.CheckBlock(p.raddr.IP.String()) {
		this.log.Infof("[Duat] Node %s exceeded rate limit, dropping packet", p.raddr.IP.String())
		return
	}
	*/
	if p.b[0] != 'd' {
		this.log.Infof("[Duat] Node %s send malformed packet, dropped", p.raddr.IP.String())
		return
	}
	r, err := readResponse(p, this.log)
	if err != nil {
		this.log.Errorf("[Duat] read response from %s error: %s", p.raddr.IP.String(), err)
		return
	}
	switch {
	case r.Y == "r":
		if bogusId(r.R.Id) {
			this.log.Infof("[Duat] received packet with bogus node id %x from %s", r.R.Id, p.raddr.IP.String())
			return
		}
		if r.R.Id == this.NodeIdStr {
			this.log.Infof("[Duat] received response from self, id %x", r.A.Id)
			return
		}
		node, addr, existed, err := this.routingTable.hostPortToNode(p.raddr.String(), this.netProto)
		if err != nil {
			this.log.Errorf("[Duat] hostPortToNode %s failed: %s", p.raddr.String(), err)
			return
		}
		if !existed {
			this.log.Errorf("[Duat] received response from a host we don't know: %v", p.raddr)
			if this.routingTable.length() < MaxNodes {
				this.requestPingIP(addr)
			}
			return
		}
		if node.id == "" {
			node.id = r.R.Id
			this.routingTable.update(node, this.netProto)
			this.log.Infof("[Duat] update node id for %s", p.raddr.IP.String())
		} else if node.id != r.R.Id {
			this.log.Infof("[Duat] node changed id from %x to %x", node.id, r.R.Id)
		}
		query, ok := node.pendingQueries[r.T]
		if !ok {
			this.log.Errorf("[Duat] received response with unknown query id(%v) from %s (%v-%v-%v)", r.T, p.raddr.String(), r.T, this.netPort, p.raddr.Port)
			return
		}
		if !node.reachable {
			this.log.Infof("[Duat] set node %x as reachable", node.id)
			node.reachable = true
		}
		if _, ok := this.routingTable.hotspots[string(query.ih)]; ok {
			needRecursion, err := this.routingTable.neighborhoodUpkeep(string(query.ih), node, this.netProto, this.peerStore)
			if err != nil {
				this.log.Errorf("[Duat] neighborhoodUpkeep error: %s", err)
			}
			_ = needRecursion
		}

		node.lastResponseTime = time.Now()
		node.pastQueries[r.T] = query

		// this.routingTable.neighborhoodUpkeep(node, this.netProto, this.peerStore)

		// If this is the first host added to the routing table, attempt a
		// recursive lookup of our own address, to build our neighborhood ASAP.
		/*
		if this.needMoreNodes() {
			this.log.Debugf("[Duat] we need more nodes")
			this.castFindNode(this.NodeId)
		}
		*/

		switch query.Type {
		case "ping":
				this.log.Debugf("[Duat] network: got ping response from %s", p.raddr.String())
				// totalRecvPingReply.Add(1)
		case "get_peers":
				this.processGetPeersResponse(node, r)
		case "find_node":
				this.processFindNodeResponse(node, r)
		case "announce_peer":
				this.log.Debugf("[Duat] network: got announce_peer response from %s", p.raddr.String())
		default:
				this.log.Errorf("[Duat] Unknown query type: %v from %v", query.Type, addr)
		}
		delete(node.pendingQueries, r.T)
		this.log.Debugf("delete query id %v-%v-%v", r.T, this.netPort, p.raddr.Port)
	case r.Y == "q":
		if r.A.Id == this.NodeIdStr {
			this.log.Infof("[Duat] received request from self, id %x", r.A.Id)
			return
		}
		node, addr, existed, err := this.routingTable.hostPortToNode(p.raddr.String(), this.netProto)
		if err != nil {
			this.log.Errorf("[Duat] hostPortToNode %s failed: %s", p.raddr.String(), err)
			return
		}
		if !existed {
			this.log.Infof("[Duat] received request from a brand new host: %v", p.raddr)
			if this.routingTable.length() < MaxNodes {
				this.requestPingIP(addr)
			}
		}
		this.log.Debugf("[Duat] get query id %v-%v-%v", r.T, p.raddr.Port, this.netPort)
		switch r.Q {
		case "ping":
			this.respondPing(p.raddr, r)
		case "get_peers":
			this.respondGetPeers(p.raddr, r)
		case "find_node":
			this.respondFindNode(p.raddr, r)
		case "announce_peer":
			this.respondAnnouncePeer(p.raddr, node, r)
		default:
			this.log.Errorf("[Duat] non-implemented handler for type %v", r.Q)
		}
	default:
		this.log.Errorf("[Duat] bogus query from %v", p.raddr)
	}
}

// low-level functions
/*
 *  send client request 
 */
func (this *Duat) castFindNode(ih InfoHash) {
	closest := this.routingTable.lookupFiltered(ih)
	if len(closest) == 0 {
		// get more nodes
		this.log.Infof("[Duat] no nodes in the table(%d), castFindNode failed for %x", this.routingTable.length(), string(ih))
		return
	}
	for _, r := range closest {
		this.requestFindNode(r, ih)
	}
}

func (this *Duat) castGetPeers(ih InfoHash) {
	closest := this.routingTable.lookupFiltered(ih)
	if len(closest) == 0 {
		// get more nodes
		this.log.Infof("[Duat] no nodes in the table(%d), castGetPeers failed for %x", this.routingTable.length(), string(ih))
		return
	}
	for _, r := range closest {
		this.requestGetPeers(r, ih)
	}
}

func (this *Duat) requestPingIP(address string) {
	r, err := this.routingTable.getOrCreateNode("", address, this.netProto)
	if err != nil {
		this.log.Errorf("[Duat] ping error for address %v: %v", address, err)
		return
	}
	this.requestPing(r)
}

func (this *Duat) requestPing(r *remoteNode) {
	t := r.newQuery("ping")
	queryArguments := map[string]interface{}{"id": this.NodeId}
	query := queryMessage{t, "q", "ping", queryArguments}
	this.log.Debugf("[Duat] network: request ping against %x@%v, distance: %x", r.id, r.address, hashDistance(InfoHash(r.id), InfoHash(this.NodeId)))
	sendMsg(this.listener, r.address, query, this.log)
	// totalSentPing.Add(1)
	this.log.Debugf("[Duat] send %p %v-%v-%v", r, query.T, this.netPort, r.address.Port)
}

func (this *Duat) requestFindNode(r *remoteNode, ih InfoHash) {
	ty := "find_node"
	transId := r.newQuery(ty)
	if _, ok := r.pendingQueries[transId]; ok {
		r.pendingQueries[transId].ih = ih
	} else {
		r.pendingQueries[transId] = &queryType{ih: ih}
	}
	queryArguments := map[string]interface{}{
		"id":     this.NodeId,
		"target": ih,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	this.log.Debugf("[Duat] network: request find_node against %x@%v, InfoHash: %x, distance: %x", r.id, r.address, ih, hashDistance(InfoHash(r.id), ih))
	r.lastSearchTime = time.Now()
	sendMsg(this.listener, r.address, query, this.log)
	this.log.Debugf("[Duat] send %p %v-%v-%v", r, query.T, this.netPort, r.address.Port)
}

func (this *Duat) requestGetPeers(r *remoteNode, ih InfoHash) {
	ty := "get_peers"
	transId := r.newQuery(ty)
	if _, ok := r.pendingQueries[transId]; ok {
		r.pendingQueries[transId].ih = ih
	} else {
		r.pendingQueries[transId] = &queryType{ih: ih}
	}
	queryArguments := map[string]interface{}{
		"id":        this.NodeId,
		"info_hash": ih,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	this.log.Debugf("[Duat] network: request get_peers against %x@%v, InfoHash: %x, distance: %x", r.id, r.address, ih, hashDistance(InfoHash(r.id), InfoHash(this.NodeId)))
	r.lastSearchTime = time.Now()
	sendMsg(this.listener, r.address, query, this.log)
	this.log.Debugf("[Duat] send %p %v-%v-%v", r, query.T, this.netPort, r.address.Port)
}

func (this *Duat) requestAnnouncePeer(address net.UDPAddr, ih InfoHash, port int, token string) {
	r, err := this.routingTable.getOrCreateNode("", address.String(), this.netProto)
	if err != nil {
		this.log.Errorf("[Duat] getOrCreateNode with %s err: %s", address.String(), err)
		return
	}
	ty := "announce_peer"
	transId := r.newQuery(ty)
	queryArguments := map[string]interface{}{
		"id":        this.NodeId,
		"info_hash": ih,
		"port":      port,
		"token":     token,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	sendMsg(this.listener, address, query, this.log)
	this.log.Debugf("[Duat] send %p %v-%v-%v", r, query.T, this.netPort, r.address.Port)
	// d.DebugLogger.Debugf("DHT: announce_peer => address: %v, ih: %x, token: %x", address, ih, token)
	this.log.Debugf("[Duat] network: request announce_peer against %x@%v, InfoHash: %x, distance: %x", 
		r.id, r.address, ih, hashDistance(InfoHash(r.id), InfoHash(this.NodeId)))

}

/*
 *  send server respond 
 */
func (this *Duat) respondPing(addr net.UDPAddr, response responseType) {
	this.log.Debugf("[Duat] network: respond ping to %v", addr)
	reply := replyMessage{
		T: response.T,
		Y: "r",
		R: map[string]interface{}{"id": this.NodeId},
	}
	sendMsg(this.listener, addr, reply, this.log)
}

func (this *Duat) respondFindNode(addr net.UDPAddr, r responseType) {
	node := InfoHash(r.A.Target)
	r0 := map[string]interface{}{"id": this.NodeId}
	reply := replyMessage{
		T: r.T,
		Y: "r",
		R: r0,
	}

	neighbors := this.routingTable.lookupFiltered(node)
	if len(neighbors) < kNodes {
		neighbors = append(neighbors, this.routingTable.lookup(node)...)
	}
	n := make([]string, 0, kNodes)
	for _, r := range neighbors {
		n = append(n, r.id+r.addressBinaryFormat)
		if len(n) == kNodes {
			break
		}
	}
	this.log.Debugf("[Duat] network: respond find_node to Host: %v, nodeId: %x, target ID: %x, distance: %x",
			addr, r.A.Id, r.A.Target, hashDistance(InfoHash(r.A.Target), InfoHash(this.NodeId)))
	reply.R["nodes"] = strings.Join(n, "")
	sendMsg(this.listener, addr, reply, this.log)
}

func (this *Duat) respondGetPeers(addr net.UDPAddr, r responseType) {
	ih := r.A.InfoHash
	r0 := map[string]interface{}{"id": this.NodeId, "token": genToken(addr, this.tokenSecrets[0])}
	reply := replyMessage{
		T: r.T,
		Y: "r",
		R: r0,
	}


	_, isMgnted := this.routingTable.hsMgnted[string(ih)]
	// if isMgnted {

	peerContacts := this.peerStore.peerContacts(ih)
	if len(peerContacts) > 0 {
		reply.R["values"] = peerContacts
		//this.log.Debugf("[Duat] network: respond get_peers(%v) to Host: %v , nodeID: %x , InfoHash: %x , distance: %x. Answer(values): %d",
		//		isMgnted, addr, r.A.Id, InfoHash(r.A.InfoHash), hashDistance(r.A.InfoHash, InfoHash(this.NodeId)), len(peerContacts))
		// sendMsg(this.listener, addr, reply, this.log)
	} 

	// no peers stored, find some nodes to return
	n := make([]string, 0, kNodes)
	for _, r := range this.routingTable.lookup(ih) {
		// XXX wtf
		// r is nil when the node was filtered.
		if r != nil {
			binaryHost := r.id + nettools.DottedPortToBinary(r.address.String())
			if binaryHost == "" {
				this.log.Debugf("[Duat] killing node with bogus address %v", r.address.String())
				this.routingTable.kill(r, this.peerStore)
			} else {
				n = append(n, binaryHost)
			}
		}
	}
	reply.R["nodes"] = strings.Join(n, "")
	this.log.Debugf("[Duat] network: respond get_peers(%v) to Host: %v , nodeID: %x , InfoHash: %x , distance: %x. Answer: values(%d) nodes(%d)",
		isMgnted, addr, r.A.Id, InfoHash(r.A.InfoHash), hashDistance(r.A.InfoHash, InfoHash(this.NodeId)), len(peerContacts), len(n))
	sendMsg(this.listener, addr, reply, this.log)
}

func (this *Duat) respondAnnouncePeer(addr net.UDPAddr, node *remoteNode, r responseType) {
	ih := InfoHash(r.A.InfoHash)
	this.log.Debugf("[Duat] network: respond announce_peer to Host: %v, nodeID: %x, infoHash: %x, peerPort %d, distance to me %x",
			addr, r.A.Id, ih, r.A.Port, hashDistance(ih, InfoHash(this.NodeId)),
	)
	// node can be nil if, for example, the server just restarted and received an announce_peer
	// from a node it doesn't yet know about.
	if node != nil && checkToken(this.tokenSecrets, addr, r.A.Token) {
		/*
		if _, isMgnted := this.routingTable.hsMgnted[string(ih)]; !isMgnted {
			this.log.Errorf("[Duat] get an announce_peer request for unmanaged data %x from %v", string(ih), addr)
		} else
		*/
		{
			this.log.Infof("[Duat] receive a new peer for %x from %v", string(ih), addr)
			peerAddr := net.TCPAddr{IP: addr.IP, Port: r.A.Port}
			this.peerStore.addContact(ih, nettools.DottedPortToBinary(peerAddr.String()))
			this.log.Debugf("addContact %x to %x\n", nettools.DottedPortToBinary(peerAddr.String()), string(ih))
				// Allow searching this node immediately, since it's telling us
				// it has an infohash. Enables faster upgrade of other nodes to
				// "peer" of an infohash, if the announcement is valid.
			node.lastResponseTime = time.Now().Add(-searchRetryPeriod)

			/*
			port := this.peerStore.hasLocalDownload(ih)
			if port != 0 {
				// XXX 
				// this.PeersRequestResults <- map[InfoHash][]string{ih: {nettools.DottedPortToBinary(peerAddr.String())}}
				this.log.Infof("[Duat] get new peer by announce_peer, wtf does this mean?")
			}
			*/
		}
	}
	// Always reply positively. jech says this is to avoid "back-tracking", not sure what that means.
	reply := replyMessage{
		T: r.T,
		Y: "r",
		R: map[string]interface{}{"id": this.NodeId},
	}
	sendMsg(this.listener, addr, reply, this.log)
}

/*
 *  process server respond 
 */
 func (this *Duat) processFindNodeResponse(node *remoteNode, resp responseType) {
	var nodelist string
	query, _ := node.pendingQueries[resp.T]
	if this.netProto == "udp6" {
		nodelist = resp.R.Nodes6
	} else {
		nodelist = resp.R.Nodes
	}
	
	if nodelist == "" {
		this.log.Debugf("[Duat] network: got find_node response from %s, len(nodelist)=%d", nettools.BinaryToDottedPort(node.addressBinaryFormat), 0)
		return
	}

	parsedNodeList := parseNodesString(nodelist, this.netProto, this.log)
	this.log.Debugf("[Duat] network: got find_node response for %x from %s, len(nodelist)=%d", query.ih, nettools.BinaryToDottedPort(node.addressBinaryFormat), len(parsedNodeList))
	for id, address := range parsedNodeList {
		if id == this.NodeIdStr {
			this.log.Debugf("[Duat] got reference of self for find_node, id %x", id)
			continue
		}
		_, addr, existed, err := this.routingTable.hostPortToNode(address, this.netProto)
		if err != nil {
			this.log.Errorf("[Duat] parsing node(hostPortToNode) from find_find response failed: %v", err)
			continue
		}
		if addr == node.address.String() {
			// SelfPromotions are more common for find_node. They are
			// happening even for router.bittorrent.com
			this.log.Infof("[Duat] got self-promotion for find_node, id %x", id)
			continue
		}
		if existed {
			this.log.Debugf("[Duat] DUPE node reference, query %x: %x@%v from %x@%v. Distance: %x.",
					query.ih, id, address, node.id, node.address, hashDistance(query.ih, InfoHash(node.id)))
			continue
		} 
		this.log.Infof("[Duat] got new node reference, query %x: %x@%v from %x@%v. Distance: %x.",
				query.ih, id, address, node.id, node.address, hashDistance(query.ih, InfoHash(node.id)))
		// Includes the node in the routing table and ignores errors.
		// Only continue the search if we really have to.
		r, err := this.routingTable.getOrCreateNode(id, addr, this.netProto)
		if err != nil {
			this.log.Errorf("[Duat] ProcessFindNodeResponse getOrCreateNode failed: %v. Id=%x, Address=%q", err, id, addr)
			continue
		}
		/* recursive lookup */
		this.requestFindNode(r, query.ih)
		/*
		needRecursion, err := this.routingTable.neighborhoodUpkeep(string(query.ih), r, this.netProto, this.peerStore)
		if err != nil {
			this.log.Errorf("[Duat] ProcessFindNodeResponse neighborhoodUpkeep failed for %x, err: %s", query.ih, err)
			continue
		}
		if !needRecursion {
			this.log.Debugf("[Duat] infohash %x final node: %x", string(query.ih), string(r.id))
			continue
		}
		this.log.Infof("[Duat] recursively request find_node against %s for InfoHash %x", r.address.String(), query.ih)
		this.requestFindNode(r, query.ih)
		*/
	 }
}

 func (this *Duat) processGetPeersResponse(node *remoteNode, resp responseType) {
	query, _ := node.pendingQueries[resp.T]
	/*
	port := this.peerStore.hasLocalDownload(query.ih)
	if port != 0 {
		this.requestAnnouncePeer(node.address, query.ih, port, resp.R.Token)
	}
	*/
	if this.outable {
		this.log.Debugf("[Duat] I'm outable, proudly announcing that")
		this.requestAnnouncePeer(node.address, query.ih, this.netPort, resp.R.Token)
	}
	if resp.R.Values != nil {
		peers := make([]string, 0)
		for _, peerContact := range resp.R.Values {
			// send peer even if we already have it in store
			// the underlying client does/should handle dupes
			this.peerStore.addContact(query.ih, peerContact)
			this.log.Debugf("addContact %x to %x\n", peerContact, string(query.ih))
			peers = append(peers, peerContact)
		}
		this.log.Debugf("[Duat] get new peer by get_peer response, totally %d", len(peers))
		if len(peers) > 0 {
			// Finally, new peers.
			result := map[InfoHash][]string{query.ih: peers}
			_ = result
			// totalPeers.Add(int64(len(peers)))
			// this.log.Debugf("DHT: processGetPeerResults, totalPeers: %v", totalPeers.String())
			/*
			select {
				case d.PeersRequestResults <- result:
				case <-d.stop:
					// if we're closing down and the caller has stopped reading
					// from PeersRequestResults, drop the result.
			}
			*/
		}
	}
	var nodelist string

	if this.netProto == "udp4" {
			nodelist = resp.R.Nodes
	} else if this.netProto == "udp6" {
			nodelist = resp.R.Nodes6
	}
	if nodelist == "" {
		this.log.Debugf("[Duat] network: got get_peers response for %x from %s, len(values)=%d, len(nodes)=%d", query.ih, nettools.BinaryToDottedPort(node.addressBinaryFormat), len(resp.R.Values), 0)
		return
	}

	parsedNodeList := parseNodesString(nodelist, this.netProto, this.log)
	this.log.Debugf("[Duat] network: got get_peers response for %x from %s, len(values)=%d, len(nodes)=%d", query.ih, nettools.BinaryToDottedPort(node.addressBinaryFormat), len(resp.R.Values), len(parsedNodeList))
	for id, address := range parsedNodeList {
		if id == this.NodeIdStr {
			this.log.Debugf("[Duat] got reference of self for find_node, id %x", id)
			continue
		}
		_, addr, existed, err := this.routingTable.hostPortToNode(address, this.netProto)
		if err != nil {
			this.log.Errorf("[Duat] parsing node(hostPortToNode) from find_find response failed: %v", err)
			continue
		}
		if addr == node.address.String() {
			// SelfPromotions are more common for find_node. They are
			// happening even for router.bittorrent.com
			this.log.Infof("[Duat] got self-promotion for find_node, id %x", id)
			continue
		}
		if existed {
			this.log.Debugf("[Duat] DUPE node reference, query %x: %x@%v from %x@%v. Distance: %x.",
					query.ih, id, address, node.id, node.address, hashDistance(query.ih, InfoHash(node.id)))
			continue
		}
		this.log.Debugf("[Duat] got new node reference, query %x: %x@%v from %x@%v. Distance: %x.",
				query.ih, id, address, node.id, node.address, hashDistance(query.ih, InfoHash(node.id)))

		r, err := this.routingTable.getOrCreateNode(id, addr, this.netProto)
		if err != nil {
			this.log.Debugf("[Duat] ProcessGetPeersResponse getOrCreateNode failed: %v. Id=%x, Address=%q", err, id, addr)
			continue
		}
		this.log.Debugf("[Duat] getOrCreateNode with %s", addr)
		
		/* recursive lookup */
		this.requestGetPeers(r, query.ih)
		/*
		needRecursion, err := this.routingTable.neighborhoodUpkeep(string(query.ih), r, this.netProto, this.peerStore)
		if err != nil {
			this.log.Errorf("[Duat] ProcessGetPeersResponse neighborhoodUpkeep failed for %x, err: %s", query.ih, err)
			continue
		}
		if !needRecursion {
			continue
		}
		this.log.Infof("[Duat] recursively request get_peers against %s for InfoHash %x", r.address.String(), query.ih)
		this.requestGetPeers(r, query.ih)
		*/
	}
 }

 /*
  * utils
  */
// XXX move function & config to routing table?
func (this *Duat) needMoreNodes() bool {
	n := this.routingTable.length()
	return n < MinNodes || n*2 < MaxNodes
}
