package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"reflect"
	"strings"
	"time"
)

type DKey struct {
	KeyA, RelA string
}

type Request struct {
	KeyRel     DKey
	Val        map[string]interface{}
	Permission string
}

type Response struct {
	Tripair Triplet
	Done    bool
	Err     error
}

type ListRequest struct {
	SourceId int
	Interval int64
}

type Triplet struct {
	Key, Rel string
	Val      map[string]interface{}
}

type ListResponse struct {
	List []string
	Err  error
}

type ListResponseT struct {
	List []Triplet
	Err  error
}

type NodeInfo struct {
	Chordid int
	Address string
}

type SendDict struct {
	Dict3 map[DKey]map[string]interface{}
}

type DIC3 struct{}

var dict3 map[DKey]map[string]interface{}
var configMap map[string]interface{}
var protocol string
var ipAdd string
var dict3File string
var methods []interface{}
var port string
var confLoc string
var successor NodeInfo
var predecessor NodeInfo
var shakey string
var chordid int
var selfaddr string
var fingertable map[int]NodeInfo
var stabilizeChan chan struct{}
var checkPredChan chan struct{}
var fixfingersChan chan struct{}
var purgeInterval float64
var stabilizeInterval float64
var checkPredInterval float64
var fixfingersInterval float64
var next int
var entrypt string

//server shutdown
func (t *DIC3) Shutdown(dKey *DKey, reply *int) error {
	fmt.Println("shuting server down...")
	*reply = 9
	if successor.Chordid == chordid {
		persistDict3()
	} else {
		InsertDict3ToSuccessor()
	}
	close(stabilizeChan)
	close(checkPredChan)
	close(fixfingersChan)

	updateSuccessorOfP()
	updatePredecessorOfS()
	os.Exit(0)
	return nil
}

// Send DICT3 entries to the successor before exit
func InsertDict3ToSuccessor() {
	if successor.Chordid != chordid {
		client, err := jsonrpc.Dial(protocol, successor.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var reply2 int
		req := SendDict{dict3}
		RpcCall := client.Go("DIC3.RPCInsertDict3ToSuccessor", req, &reply2, nil)
		replyCall := <-RpcCall.Done
		if replyCall != nil {
		}
		client.Close()
	}
}

//Insert DICT3 in successor
func (t *DIC3) RPCInsertDict3ToSuccessor(dict *SendDict,
	reply *int) error {
	for k, v := range dict.Dict3 {
		dict3[k] = v
	}
	*reply = 1
	return nil
}

// get DICT3 entries from successor after joining ring
func getDict3FromSuccessor() error{
        if successor.Chordid != chordid {
                client, err := jsonrpc.Dial(protocol, successor.Address)
                if err != nil {
                        log.Fatal("dialing:", err)
                }
                var reply2 []string
                req := chordid
                RpcCall := client.Go("DIC3.RPCGetDict3FromSuccessor", req, &reply2,nil)
                replyCall := <-RpcCall.Done
                if replyCall != nil {
                }
                client.Close()
                
                //iterate through reply2 and insert in my dict3
                for _,triplet := range reply2{
                	arr := strings.Split(triplet, "=")
					if len(arr) == 3 {
						key_rel := DKey{arr[0], arr[1]}
						b := []byte(arr[2])
						var f map[string]interface{}
						err := json.Unmarshal(b, &f)
						if err != nil {
							log.Fatal(err)
						}
						dict3[key_rel] = f
					}
                }
                fmt.Println("Updated my DICT3, added triplets - ", len(dict3))
        }
        return nil
}

//Get DICT3 from successor
func (t *DIC3) RPCGetDict3FromSuccessor(pchordid *int,
        reply *[]string) error {
        fmt.Println("I am in RPCGetDict3FromSuccessor", *pchordid, *reply)
        myDict := make([]string,1)
        for k, v := range dict3 {
                cid := getKeyRelHash(k.KeyA,k.RelA)
                if !belongsto(cid,*pchordid,chordid) {
                	b, err := json.Marshal(v)
                	checkError(err)
					val := string(b[:])
					s := k.KeyA+ "="+ k.RelA+ "="+ val
					myDict = append(myDict, s)
					//remove from my dict3
					delete(dict3, k)
                }
        }
        *reply = myDict
        return nil
}

// Update successor of predecessor before leaving.
func updateSuccessorOfP() {
	np := predecessor
	if np.Chordid != chordid {
		client, err := jsonrpc.Dial(protocol, np.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var reply2 int
		RpcCall := client.Go("DIC3.RPCUpdateSuccessorOfP", successor, &reply2, nil)
		replyCall := <-RpcCall.Done
		if replyCall != nil {
		}
		client.Close()
	}
}

//RPC for updating successor of predecessor.
func (t *DIC3) RPCUpdateSuccessorOfP(request NodeInfo, reply *int) error {
	successor.Chordid = request.Chordid
	successor.Address = request.Address
	*reply = 1
	return nil
}

// Update predecessor of successor before leaving.
func updatePredecessorOfS() {
	if successor.Chordid != chordid {
		client, err := jsonrpc.Dial(protocol, successor.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var reply2 int
		RpcCall := client.Go("DIC3.RPCUpdatePredecessorOfS", predecessor, &reply2, nil)
		replyCall := <-RpcCall.Done
		if replyCall != nil {
		}
		client.Close()
	}
}

// RPC for updating the predecessor for successor
func (t *DIC3) RPCUpdatePredecessorOfS(request NodeInfo, reply *int) error {
	predecessor.Chordid = request.Chordid
	predecessor.Address = request.Address
	*reply = 1
	return nil
}

//Lookup returns value stored for given key and relation
func (t *DIC3) Lookup(req *Request, reply *ListResponseT) error {
	//fmt.Println("in Lookup : ", req.KeyRel.KeyA, req.KeyRel.RelA)
	result := make([]Triplet, 1)

	if len(strings.TrimSpace(req.KeyRel.RelA)) == 0 {
		//partial key lookup
		fmt.Println("Relation is missing")

		//find in my dict3
		for k, v := range dict3 {
			if strings.EqualFold(k.KeyA, req.KeyRel.KeyA) {
				v["accessed"] = time.Now().Format(time.RFC3339)
				result = append(result, Triplet{k.KeyA, k.RelA, v})
			}
		}

		//after that forward
		if req.Permission == "" {
			khash := getKeyHash(req.KeyRel.KeyA)
			set := make(map[NodeInfo]bool)
			for _, kh := range khash {
				set[findSuccessorFT(kh)] = true
			}
			req.Permission = "FWD"
			fmt.Println(set)

			for ks, _ := range set {
				if ks.Chordid == chordid {
					// searched in my dict3
				} else {
					fmt.Println("RPC ing lookup... to ", ks)
					client, err := jsonrpc.Dial(protocol, ks.Address)
					if err != nil {
						log.Fatal("dialing:", err)
					}
					var reply2 ListResponseT
					RpcCall := client.Go("DIC3.Lookup", req, &reply2, nil)
					replyCall := <-RpcCall.Done

					if replyCall != nil {
					}

					result2 := reply2.List

					for _, i := range result2 {
						result = append(result, i)
					}

					client.Close()
				}
			}
		}

		reply.List = result
		return nil

	} else if len(strings.TrimSpace(req.KeyRel.KeyA)) == 0 {

		//partial rel lookup
		fmt.Println("Key is missing")

		//find in my dict3
		for k, v := range dict3 {
			if strings.EqualFold(k.RelA, req.KeyRel.RelA) {
				v["accessed"] = time.Now().Format(time.RFC3339)
				result = append(result, Triplet{k.KeyA, k.RelA, v})
			}
		}

		//after that forward
		if req.Permission == "" {
			rhash := getRelHash(req.KeyRel.RelA)
			set := make(map[NodeInfo]bool)
			for _, kh := range rhash {
				if !belongsto(kh, predecessor.Chordid, chordid) {
					set[findSuccessorFT(kh)] = true
				}
			}
			req.Permission = "FWD"
			fmt.Println(set)

			for rh, _ := range set {
				if rh.Chordid == chordid {
					// searched in my dict3
				} else {
					client, err := jsonrpc.Dial(protocol, rh.Address)
					if err != nil {
						log.Fatal("dialing:", err)
					}
					var reply2 ListResponseT
					RpcCall := client.Go("DIC3.Lookup", req, &reply2, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					result2 := reply2.List
					for _, i := range result2 {
						result = append(result, i)
					}
					client.Close()
				}
			}
		}

		reply.List = result
		return nil

	} else {
		//both key relation given
		val := dict3[req.KeyRel]
		fmt.Println(val, len(dict3))
		if val != nil {
			val["accessed"] = time.Now().Format(time.RFC3339)
			result = append(result, Triplet{req.KeyRel.KeyA, req.KeyRel.RelA, val})
			for _, rr := range result {
				reply.List = append(reply.List, rr)
			}
			return nil
		} else {
			//if its not a single node on ring
			if successor.Chordid != chordid {

				krhash := getKeyRelHash(req.KeyRel.KeyA, req.KeyRel.RelA)
				if krhash == chordid {
					reply.Err = nil
					return nil
				}
				if !belongsto(krhash, predecessor.Chordid, chordid) {
					service := findSuccessorFT(krhash)
					client, err := jsonrpc.Dial(protocol, service.Address)
					if err != nil {
						log.Fatal("dialing:", err)
					}

					var reply2 ListResponseT
					RpcCall := client.Go("DIC3.Lookup", req, &reply2, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					result2 := reply2.List
					fmt.Println(result2)
					for _, i := range result2 {
						result = append(result, i)
					}

					fmt.Println(reply)
					client.Close()
				} else {
					fmt.Println("Key-Rel not found on CHORD ring...")
					return nil
				}
			}
		}
		reply.List = result
		return nil
	}
} //lookup ends here

//Insert a given triplet in DICT3
func (t *DIC3) Insert(triplet *Request, reply *Response) error {
	fmt.Println("in Insert : ", triplet.KeyRel.KeyA, triplet.KeyRel.RelA,
		triplet.Val)

	hashid := getKeyRelHash(triplet.KeyRel.KeyA, triplet.KeyRel.RelA)
	//fmt.Println("Debug: Predecessor:", predecessor.Chordid)
	if belongsto(hashid, predecessor.Chordid, chordid) == true {
		val := dict3[triplet.KeyRel]
		if val != nil {
			fmt.Println("DKey already exists -", triplet.KeyRel)
			reply.Done = false
			return nil
		}
		insertTripletToDict3(triplet.KeyRel, triplet.Val, triplet.Permission)
		_, ok := dict3[DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}]
		reply.Done = ok
		reply.Err = nil
		return nil
	}
	np := nearestPredecessor(hashid)
	client, err := jsonrpc.Dial(protocol, np.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply2 Response
	RpcCall := client.Go("DIC3.Insert", triplet, &reply2, nil)
	replyCall := <-RpcCall.Done
	if replyCall != nil {
	}

	reply.Done = reply2.Done
	reply.Err = reply2.Err
	fmt.Println(reply)
	client.Close()
	return nil
}

// Routine for inserting Triplet to dict3
func insertTripletToDict3(dkey DKey, val map[string]interface{}, perm string) {
	valJson := make(map[string]interface{})
	valJson["content"] = val
	valJson["size"] = reflect.TypeOf(val).Size()
	valJson["created"] = time.Now().Format(time.RFC3339)
	valJson["accessed"] = time.Now().Format(time.RFC3339)
	valJson["modified"] = time.Now().Format(time.RFC3339)
	valJson["permission"] = perm

	dict3[DKey{dkey.KeyA, dkey.RelA}] = valJson
}

//InsertOrUpdate given triplet in DICT3
func (t *DIC3) InsertOrUpdate(triplet *Request, reply *Response) error {
	fmt.Println("in InsertOrUpdate : ", triplet.KeyRel.KeyA, triplet.KeyRel.RelA, triplet.Val)

	hashid := getKeyRelHash(triplet.KeyRel.KeyA, triplet.KeyRel.RelA)
	if belongsto(hashid, predecessor.Chordid, chordid) == true {
		keyRel := DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}
		v, ok := dict3[keyRel]
		if !ok {
			//Insert
			fmt.Println("Inserting.....")
			//dict3[keyRel] = triplet.Val
			insertTripletToDict3(triplet.KeyRel, triplet.Val, triplet.Permission)
		} else {
			//Update
			fmt.Println("Updating.....")
			access := v["permission"].(string)
			if strings.EqualFold("RW", access) {
				v["content"] = triplet.Val
				v["size"] = reflect.TypeOf(triplet.Val).Size()
				v["modified"] = time.Now().Format(time.RFC3339)
				_, ok = dict3[DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}]
			} else {
				fmt.Println("No RW access")
			}
		}

		reply.Done = ok
		reply.Err = nil
		return nil
	}
	np := nearestPredecessor(hashid)
	client, err := jsonrpc.Dial(protocol, np.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply2 Response
	RpcCall := client.Go("DIC3.InsertOrUpdate", triplet, &reply2, nil)
	replyCall := <-RpcCall.Done
	if replyCall != nil {
	}

	reply.Done = reply2.Done
	reply.Err = reply2.Err
	fmt.Println(reply)
	client.Close()
	return nil

}

//Delete from DICT3
func (t *DIC3) Delete(req *Request, reply *Response) error {
	fmt.Println("in Delete : ", req.KeyRel.KeyA, req.KeyRel.RelA)

	v, ok := dict3[DKey{req.KeyRel.KeyA, req.KeyRel.RelA}]
	if ok {
		access := v["permission"].(string)
		fmt.Println("Access level : ", access)
		if strings.EqualFold("RW", access) {
			delete(dict3, req.KeyRel)
			fmt.Println("after delete DICT3 ", dict3)

		} else {
			fmt.Println("No RW access!!")
		}
		reply.Done = true
		reply.Err = nil
		return nil
	}

	if successor.Chordid != chordid {
		hashid := getKeyRelHash(req.KeyRel.KeyA, req.KeyRel.RelA)
		if hashid == chordid {
			reply.Err = nil
			return nil
		}
		if !belongsto(hashid, predecessor.Chordid, chordid) {
			np := nearestPredecessor(hashid)
			client, err := jsonrpc.Dial(protocol, np.Address)
			if err != nil {
				log.Fatal("dialing:", err)
			}
			var reply2 Response
			RpcCall := client.Go("DIC3.Delete", req, &reply2, nil)
			replyCall := <-RpcCall.Done
			if replyCall != nil {
			}

			reply.Done = reply2.Done
			reply.Err = reply2.Err
			fmt.Println(reply)
			client.Close()
		} else {
			fmt.Println("Key-Rel not found on CHORD ring for Deletion...")
			return nil
		}
	}
	return nil
}

//list keys in DICT3
func (t *DIC3) ListKeys(req *ListRequest, reply *ListResponse) error {
	var req2 ListRequest
	if req.SourceId == -1 {
		req2.SourceId = chordid
	} else {
		req2.SourceId = req.SourceId
	}
	if successor.Chordid == req.SourceId || successor.Chordid == chordid {
		keys := make([]string, 1)
		for k := range dict3 {
			found := checkIfPresent(k.KeyA, keys)
			if !found {
				keys = append(keys, k.KeyA)
			}
		}
		reply.List = keys
		reply.Err = nil
		return nil
	} else {
		client, err := jsonrpc.Dial(protocol, successor.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var reply2 ListResponse
		RpcCall := client.Go("DIC3.ListKeys", req2, &reply2, nil)
		replyCall := <-RpcCall.Done
		if replyCall != nil {
		}
		client.Close()
		keys := make([]string, 1)
		for k, _ := range dict3 {
			found := checkIfPresent(k.KeyA, keys)
			if !found {
				keys = append(keys, k.KeyA)
			}
		}
		for _, j := range reply2.List {
			found := checkIfPresent(j, keys)
			if !found {
				keys = append(keys, j)
			}
		}
		reply.List = keys
		reply.Err = nil
		return nil
	}
	return nil
}

//list key-relation pairs in DICT3
func (t *DIC3) ListIDs(req *ListRequest, reply *ListResponse) error {
	var req2 ListRequest
	if req.SourceId == -1 {
		req2.SourceId = chordid
	} else {
		req2.SourceId = req.SourceId
	}
	if successor.Chordid == req.SourceId || successor.Chordid == chordid {
		keys := make([]string, 1)
		for k := range dict3 {
			keys = append(keys, "[", k.KeyA, ",", k.RelA, "]")
		}
		reply.List = keys
		reply.Err = nil
		return nil
	} else {
		client, err := jsonrpc.Dial(protocol, successor.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var reply2 ListResponse
		RpcCall := client.Go("DIC3.ListIDs", &req2, &reply2, nil)
		replyCall := <-RpcCall.Done
		if replyCall != nil {
		}
		client.Close()
		keys := make([]string, 1)
		for k, _ := range dict3 {
			keys = append(keys, "[", k.KeyA, ",", k.RelA, "]")
		}
		for _, j := range reply2.List {
			keys = append(keys, j)
		}
		reply.List = keys
		reply.Err = nil
		return nil
	}
	return nil
}

// Update predecessor if more up-to-date.
func (t *DIC3) Notify(request NodeInfo, reply *NodeInfo) error {
	if predecessor.Chordid == -1 ||
		belongsto(request.Chordid, predecessor.Chordid, chordid) == true {
		predecessor.Chordid = request.Chordid
		predecessor.Address = request.Address
		return nil
	}
	return nil
}

// Find position on the ring and return successor information.
func (t *DIC3) FindPlaceOnRing(request *NodeInfo, reply *NodeInfo) error {
	if belongsto(request.Chordid, chordid, successor.Chordid) == true {
		prevSuc := successor
		reply.Chordid = prevSuc.Chordid
		reply.Address = prevSuc.Address
		successor.Chordid = request.Chordid
		successor.Address = request.Address
		fingertable[1] = successor
		fmt.Println("Updated successor as ", successor)
		return nil
	} else {
		np := nearestPredecessor(request.Chordid)
		client, err := jsonrpc.Dial(protocol, np.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var reply2 NodeInfo
		req2 := NodeInfo{request.Chordid, request.Address}
		RpcCall := client.Go("DIC3.FindPlaceOnRing", &req2, &reply2, nil)
		replyCall := <-RpcCall.Done
		if replyCall != nil {
		}
		client.Close()
		reply.Address = reply2.Address
		reply.Chordid = reply2.Chordid
		return nil
	}
	return nil
}

// Returns true if num belongs to range start and end on the ring
func belongsto(num int, start int, end int) bool {
	diff1 := end - start
	if diff1 <= 0 {
		diff1 = diff1 + 32
	}
	diff2 := num - start
	if diff2 <= 0 {
		diff2 = diff2 + 32
	}
	if diff1 >= diff2 {
		return true
	}
	return false
}

//return predcessor
func (t *DIC3) Predecessor(me NodeInfo, pred *NodeInfo) error {
	pred.Chordid = predecessor.Chordid
	pred.Address = predecessor.Address
	return nil
}

//process for purging unused triplets
func (t *DIC3) PurgeUnusedTriplets(req ListRequest, reply *int) error {
	fmt.Println("Purging Unused triplets ")
	var req2 ListRequest
	if req.SourceId == -1 {
		req2.SourceId = chordid
	} else {
		req2.SourceId = req.SourceId
	}
	interval := req.Interval * 1000000000
	for k, v := range dict3 {
		access := v["permission"].(string)
		if strings.EqualFold("RW", access) {
			access_time, err := time.Parse(time.RFC3339, v["accessed"].(string))
			checkError(err)
			duration := time.Since(access_time)
			var durationAsInt64 = int64(duration)
			if durationAsInt64 > interval {
				//delete triplet
				fmt.Println("Deleting ", k)
				delete(dict3, k)
			}
		}
	}

	if successor.Chordid == req.SourceId || successor.Chordid == chordid {
		fmt.Println(req.SourceId, " ", successor.Chordid)
		return nil
	} else {
		client, err := jsonrpc.Dial(protocol, successor.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var reply2 int
		RpcCall := client.Go("DIC3.PurgeUnusedTriplets", &req2, &reply2, nil)
		replyCall := <-RpcCall.Done
		if replyCall != nil {
		}
		client.Close()
		return nil
	}

	return nil
}

//background process
//called periodically; verifies nodes immidiate successor
//and tell successor about node
func stabilize() {
	stabilizeTicker := time.NewTicker(time.Duration(stabilizeInterval) * time.Second)
	stabilizeChan = make(chan struct{})
	go func() {
		for {
			select {

			case <-stabilizeTicker.C:
				client, err := jsonrpc.Dial(protocol, successor.Address)
				checkError(err)
				var x NodeInfo
				RpcCall := client.Go("DIC3.Predecessor", successor, &x, nil)
				replyCall := <-RpcCall.Done
				if replyCall != nil {
				}

				if belongsto(x.Chordid, chordid, successor.Chordid) {
					//update this node's successor
					//successor = x
				}

				//tell successor- new node, that this node id
				//its predecessor
				newreq := NodeInfo{chordid, selfaddr}
				RpcCall = client.Go("DIC3.Notify", newreq, nil, nil)
				replyCall = <-RpcCall.Done
				if replyCall != nil {
				}
				client.Close()

			case <-stabilizeChan:
				stabilizeTicker.Stop()
				return
			}
		}
	}()
}

//called periodically. checks whether predecessor has failed
func check_predecessor() {

	checkPredTicker := time.NewTicker(time.Duration(checkPredInterval) *
		time.Second)
	checkPredChan = make(chan struct{})
	go func() {
		for {
			select {
			case <-checkPredTicker.C:
				client, err := jsonrpc.Dial(protocol, predecessor.Address)
				if err != nil {
					//predecessor has failed
					predecessor.Chordid = -1
				}
				client.Close()

			case <-checkPredChan:
				checkPredTicker.Stop()
				return
			}
		}
	}()
}

// Main function
func main() {

	loadConfig()
	loadDict3()
	fingertable = make(map[int]NodeInfo)
	next = 0
	if entrypt == "null" {
		createChordRing()
	} else {
		joinChordRing(entrypt)
	}

	stabilize()
	check_predecessor()
	fixFingers()

	getDict3FromSuccessor()
	
	dic3 := new(DIC3)
	rpc.Register(dic3)

	tcpAddr, err := net.ResolveTCPAddr(protocol, port)
	checkError(err)
	fmt.Println("Server started........")
	listener, err := net.ListenTCP(protocol, tcpAddr)
	checkError(err)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		jsonrpc.ServeConn(conn)
	}
}

//load config in configMap
func loadConfig() error {
	configMap = make(map[string]interface{})
	fmt.Println("Reading ", os.Args[1])
	dat, err := ioutil.ReadFile(os.Args[1])
	checkError(err)

	if err := json.Unmarshal(dat, &configMap); err != nil {
		log.Fatal("Error in loading config ", err)
	}
	protocol = configMap["protocol"].(string)
	ipAdd = configMap["ipAddress"].(string)
	port = configMap["port"].(string)
	addr := []string{ipAdd, port}
	selfaddr = strings.Join(addr, "")
	if selfaddr == "" {
		fmt.Println("Could not initialize selfaddr")
	}
	chordid = getChordId(port, ipAdd)
	persiStorage :=
		configMap["persistentStorageContainer"].(map[string]interface{})
	dict3File = persiStorage["file"].(string)
	methods = configMap["methods"].([]interface{})
	stabilizeInterval = configMap["stabilizeInterval"].(float64)
	checkPredInterval = configMap["checkPredInterval"].(float64)
	fixfingersInterval = configMap["fixfingersInterval"].(float64)
	entrypt = configMap["entrypoint"].(string)
	fmt.Println("Methods exposed by server: ", methods)
	return nil
}

//load DICT3 in memory from persistent storage
func loadDict3() error {
	dict3 = make(map[DKey]map[string]interface{})

	file, err := os.Open(dict3File)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		arr := strings.Split(scanner.Text(), "=")
		if len(arr) == 3 {
			key_rel := DKey{arr[0], arr[1]}
			b := []byte(arr[2])
			var f map[string]interface{}
			err := json.Unmarshal(b, &f)
			if err != nil {
				log.Fatal(err)
			}
			dict3[key_rel] = f
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return nil
}

// Function for updating the finger table.
func fixFingers() {
	fixfingersTicker := time.NewTicker(time.Duration(fixfingersInterval) *
		time.Second)
	fixfingersChan = make(chan struct{})
	go func() {
		for {
			select {
			case <-fixfingersTicker.C:
				next = next + 1
				if next > 5 {
					next = 1
				}
				fingertable[next] = findSuccessorFT((chordid +
					int(math.Pow(2, float64(next-1)))) % 32)
				//fmt.Println("finger ", next, fingertable[next])
			case <-fixfingersChan:
				fixfingersTicker.Stop()
				return
			}
		}
	}()
}

// Finds the successor of given chord id.
func findSuccessorFT(id int) NodeInfo {
	if belongsto(id, chordid, successor.Chordid) == true {
		return successor
	} else {
		np := nearestPredecessor(id)
		client, err := jsonrpc.Dial(protocol, np.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var reply2 NodeInfo
		var reply NodeInfo
		RpcCall := client.Go("DIC3.RPCFindSuccessorFT", id, &reply2, nil)
		replyCall := <-RpcCall.Done
		if replyCall != nil {
		}
		client.Close()
		reply.Address = reply2.Address
		reply.Chordid = reply2.Chordid
		return reply
	}
}

// RPC for finding the successor of given chord id
func (t *DIC3) RPCFindSuccessorFT(request int, reply *NodeInfo) error {
	if belongsto(request, chordid, successor.Chordid) == true {
		reply.Chordid = successor.Chordid
		reply.Address = successor.Address
	} else {
		np := nearestPredecessor(request)
		client, err := jsonrpc.Dial(protocol, np.Address)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var reply2 NodeInfo
		RpcCall := client.Go("DIC3.RPCFindSuccessorFT", request,
			&reply2, nil)
		replyCall := <-RpcCall.Done
		if replyCall != nil {
		}
		client.Close()
		reply.Address = reply2.Address
		reply.Chordid = reply2.Chordid
	}
	return nil
}

// Save the contents of dict3 to local storage if the last node is leaving the ring.
func persistDict3() error {
	f, err := os.Create(dict3File)
	checkError(err)
	defer f.Close()

	for k, v := range dict3 {
		b, err := json.Marshal(v)
		val := string(b[:])
		s := []string{k.KeyA, "=", k.RelA, "=", val, "\n"}
		_, err = f.WriteString(strings.Join(s, ""))
		checkError(err)
		f.Sync()
	}
	return nil
}

// Returns the chord id for given key-relation pair
func getKeyRelHash(k string, r string) int {
	h1 := sha1.New()
	h1.Write([]byte(k))
	b1 := h1.Sum(nil)
	data1 := b1[0]
	id1 := data1 % 8
	id1 = id1 * 4

	h2 := sha1.New()
	h2.Write([]byte(r))
	b2 := h2.Sum(nil)
	data2 := b2[0]
	id2 := data2 % 4
	retid := int(id1 + id2)
	//fmt.Println("Hash for key-rel=", retid)
	return retid
}

// Returns list of vnode chord ids for partial lookup if relation is missing
func getKeyHash(k string) [4]int {
	h1 := sha1.New()
	h1.Write([]byte(k))
	b1 := h1.Sum(nil)
	data1 := b1[0]
	id1 := data1 % 8
	id1 = id1 * 4
	idint := int(id1)
	var nodelist [4]int
	for k := 0; k < 4; k++ {
		nodelist[k] = k + idint
	}

	//fmt.Println("Nodelist for given key", nodelist)
	return nodelist
}

// Returns list of vnode chord ids for partial lookup if key is missing
func getRelHash(r string) [8]int {
	h1 := sha1.New()
	h1.Write([]byte(r))
	b1 := h1.Sum(nil)
	data1 := b1[0]
	id1 := data1 % 4
	idint := int(id1)
	var nodelist [8]int
	for k := 0; k < 8; k++ {
		nodelist[k] = (k * 4) + idint
	}
	//fmt.Println("Nodelist for given relation", nodelist)
	return nodelist
}

// Returns chord id of a new node that is joining the ring.
func getChordId(po string, ip string) int {
	addr := []string{po, ip}
	idaddr := strings.Join(addr, "")

	h1 := sha1.New()
	h1.Write([]byte(idaddr))
	b1 := h1.Sum(nil)

	bb1 := int(b1[3]) + int(b1[2])*10 + int(b1[8])*20 + int(b1[12])*30
	cid := bb1 % 32

	fmt.Println("Chord id = ", cid)
	return cid
}

// Creates a new Chord ring
func createChordRing() {
	successor.Address = selfaddr
	successor.Chordid = chordid
	predecessor.Address = successor.Address
	predecessor.Chordid = successor.Chordid
	fingertable[1] = NodeInfo{chordid, selfaddr}
	fmt.Println("Created ChordRing")
}

// Joins an existing chord ring
func joinChordRing(naddr string) error {
	predecessor.Chordid = -1
	service := naddr
	client, err := jsonrpc.Dial(protocol, service)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply NodeInfo
	request := NodeInfo{chordid, selfaddr}
	RpcCall := client.Go("DIC3.FindPlaceOnRing", &request, &reply, nil)
	replyCall := <-RpcCall.Done
	if replyCall != nil {
	}
	successor.Address = reply.Address
	successor.Chordid = reply.Chordid
	fingertable[1] = NodeInfo{reply.Chordid, reply.Address}
	fmt.Println("Joined ChordRing: successor =", successor.Address)
	client.Close()
	return nil
}

// Returns address of successor for a given chord id from local finger-table data
/*func findSuccessor(nid int) string {
	var diff int
	if nid > chordid {
		diff = nid - chordid
	} else {
		diff = nid + (32 - chordid)
	}

	ind := int(math.Log2(float64(diff)))
	return fingertable[(ind + 1)].Address
}*/

// Find nearest predecessor of given chord id from finger table. 
func nearestPredecessor(nid int) NodeInfo {
	flen := len(fingertable)
	for j := flen; j > 0; j-- {
		if belongsto(fingertable[j].Chordid, chordid, nid) {
			return fingertable[j]
		}
	}
	return fingertable[1]
}

// Returns true if a key-relation is present in the dict3
func checkIfPresent(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// Error checking routine.
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
