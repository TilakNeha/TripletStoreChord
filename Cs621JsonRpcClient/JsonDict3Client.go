package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc/jsonrpc"
	"os"
	"strconv"
	"strings"
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

type ListRequest struct {
	SourceId int
	Interval int64
}

var configMap map[string]interface{}
var protocol string
var ipAdd string
var dict3File string
var methods []interface{}
var port string

func main() {

	loadConfig()

	//get Connection to server
	ls := []string{ipAdd, port}
	service := strings.Join(ls, "")
	client, err := jsonrpc.Dial(protocol, service)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	var command string
	for i := 0; command != "exit"; i = i {
		fmt.Println("Enter request command:")
		scanner.Scan()
		command = scanner.Text()
		fmt.Println(command)

		commands := strings.Split(scanner.Text(), "(")
		if command != "exit" {

			method, par := commands[0], commands[1]
			par = par[:len(par)-1]

			switch method {

			case "lookup":
				{
					// Lookup request
					params := strings.Split(par, ",")
					dKey := DKey{strings.TrimSpace(params[0]), strings.TrimSpace(params[1])}
					req := Request{dKey, nil, ""}
					var reply ListResponseT
					RpcCall := client.Go("DIC3.Lookup", req, &reply, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					fmt.Println("lookup:: ", reply)
				}

			case "insert":
				{
					// Insert request
					params := strings.Split(par, ",")
					var v1 map[string]interface{}
					b := []byte(strings.TrimSpace(strings.Join(params[3:], ",")))
					err := json.Unmarshal(b, &v1)
					if err != nil {
						log.Fatal(err)
					}

					tri := Request{DKey{strings.TrimSpace(params[0]), strings.TrimSpace(params[1])}, v1, strings.TrimSpace(params[2])}
					var ok Response
					fmt.Println(tri)
					RpcCall := client.Go("DIC3.Insert", tri, &ok, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					fmt.Println("insert successful:: ", ok.Done)
				}

			case "insertOrUpdate":
				{
					// InsertOrUpdate request
					params := strings.Split(par, ",")
					var v1 map[string]interface{}
					b := []byte(strings.TrimSpace(strings.Join(params[3:], ",")))
					err := json.Unmarshal(b, &v1)
					if err != nil {
						log.Fatal(err)
					}
					tri := Request{DKey{strings.TrimSpace(params[0]), strings.TrimSpace(params[1])}, v1, strings.TrimSpace(params[2])}
					var ok Response
					RpcCall := client.Go("DIC3.InsertOrUpdate", tri, &ok, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					fmt.Println("insert or update executed")
				}

			case "delete":
				{
					// Delete request
					params := strings.Split(par, ",")
					dKey := DKey{strings.TrimSpace(params[0]), strings.TrimSpace(params[1])}
					req := Request{dKey, nil, ""}
					var ok Response
					RpcCall := client.Go("DIC3.Delete", req, &ok, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					fmt.Println("delete executed")
				}

			case "listKeys":
				{
					//list keys in DICT3
					var respk ListResponse
					req := ListRequest{-1, -1}
					RpcCall := client.Go("DIC3.ListKeys", req, &respk, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					fmt.Println("list keys:: ", respk)
				}

			case "listIDs":
				{
					//list key-relation pairs in DICT3
					var resp ListResponse
					req := ListRequest{-1, -1}
					RpcCall := client.Go("DIC3.ListIDs", req, &resp, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					fmt.Println("list ids:: ", resp)
				}

			case "shutdown":
				{
					//shutdown request
					var out int
					RpcCall := client.Go("DIC3.Shutdown", nil, &out, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					fmt.Println("shutdown executed..")
				}
			case "purgeUnusedTriplets":
				{
					//purge unused triplets request
					var out int
					params := strings.Split(par, ",")
					purgeInterval, _ := strconv.ParseInt(params[0], 0, 64)
					req := ListRequest{-1, purgeInterval}
					RpcCall := client.Go("DIC3.PurgeUnusedTriplets", req, &out, nil)
					replyCall := <-RpcCall.Done
					if replyCall != nil {
					}
					fmt.Println("Purge executed..")
				}
			default:
				fmt.Println("No such method named - ", method)
			}
		}
	}
}

//load config in configMap
func loadConfig() error {
	configMap = make(map[string]interface{})
	fmt.Println("Reading ", os.Args[1])
	dat, err := ioutil.ReadFile(os.Args[1])
	checkError(err)

	if err = json.Unmarshal(dat, &configMap); err != nil {
		log.Fatal("Error in loading config ", err)
	}
	protocol = configMap["protocol"].(string)
	ipAdd = configMap["ipAddress"].(string)
	port = configMap["port"].(string)

	methods = configMap["methods"].([]interface{})
	fmt.Println("Methods available to client: ", methods)
	return nil
}

//Error checking routine
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
