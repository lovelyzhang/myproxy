package main

import (
	"flag"
	"net"
	"log"
	"fmt"
	"bufio"
	"time"
	"sync"
)

var remoteIP = flag.String("rip", "127.0.0.1", "remote ip")
var remotePort = flag.Int("rport", 8888, "remote port")
var proxyIP = flag.String("nip", "127.0.0.1", "nat ip")
var proxyCtrlPort = flag.Int("nport", 8080, "nat ctrl port")
var proxyDataPort = flag.Int("niport", 8081, "nat data port")

var remoteAddr string
var proxyCtrlAddr string
var proxyDataAddr string

type ClientCtrlConn struct {
	conn       *net.TCPConn
	Alive      bool
	Mutex      sync.Mutex
	CtrlRWChan chan string
}

func NewClientCtrlConn() *ClientCtrlConn {
	clientCtrlConn := new(ClientCtrlConn)
	return clientCtrlConn
}

func (c *ClientCtrlConn) Init(conn *net.TCPConn) {
	c.Alive = true
	c.conn = conn
	c.CtrlRWChan = make(chan string)
}
func (c *ClientCtrlConn) Dead() {
	c.Mutex.Lock()
	c.Alive = false
	c.Mutex.Unlock()
	log.Println("Ctrl Connection Dead.")
}

func (c *ClientCtrlConn) CleanUp() {
	c.conn.Close()
	close(c.CtrlRWChan)
	log.Println("Clean up.")
}

func (c *ClientCtrlConn) Work() {
	var wg sync.WaitGroup
	wg.Add(2)
	go c.CtrlRead(&wg)
	go c.CtrlWrite(&wg)
	wg.Wait()
	log.Println("Done.")
}

func (c *ClientCtrlConn) CtrlRead(wg *sync.WaitGroup) {
	connReader := bufio.NewReader(c.conn)
loop:
	for {
		if c.Alive {
			c.conn.SetReadDeadline(time.Now().Add(time.Second * 10))
			if readBytes, err := connReader.ReadBytes('\n'); err == nil {
				switch string(readBytes[:len(readBytes)-1]) {
				case "HB":
					c.CtrlRWChan <- "HB"
				case "NEW":
					// 创建新连接
					log.Println("New Proxy connections.")
					if rConn, err := DialTCP(remoteAddr); err == nil {
						if pConn, err := DialTCP(proxyDataAddr); err == nil {
							go Swap(rConn, pConn)
						} else {
							log.Println("Connection to proxy connpool failed.")
							c.CtrlRWChan <- "FAIL"
						}
					} else {
						log.Println("Connection to remote connpool failed.")
						c.CtrlRWChan <- "FAIL"
					}
				}
			} else {
				log.Println(err)
				c.Dead()
				break loop
			}
		} else {
			break
		}
	}
	log.Println("Read end.")
	wg.Done()
}

func (c *ClientCtrlConn) CtrlWrite(wg *sync.WaitGroup) {
	HBTicker := time.NewTicker(3 * time.Second)
	var HBTimer *time.Timer
loop:
	for {
		if c.Alive {
			select {
			case <-HBTicker.C:
				sendBuf := []byte("HB\n")
				c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := c.conn.Write(sendBuf); err == nil {
					log.Println("Send heart beat package.")
					HBTimer = time.NewTimer(3 * time.Second)
					log.Println("heart beat timer begin.")
				} else {
					log.Println(err)
					c.Dead()
					break loop
				}
				select {
				case <-c.CtrlRWChan:
					log.Println("Recieve heart beat response package.")
					// 收到心跳回应
					HBTimer.Stop()
					log.Println("heart beat timer stop.")
					break
				case <-HBTimer.C:
					// 重新连接
					log.Println("heart beat timer timeout.")
					c.Dead()
					break loop
				}
			case s := <-c.CtrlRWChan:
				sendBuf := append([]byte(s), '\n')
				c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := c.conn.Write(sendBuf); err != nil {
					log.Println(err)
					c.Dead()
					break loop
				}
			}
		} else {
			break loop
		}
	}
	log.Println("Write end.")
	wg.Done()
}

func Copyto(des *net.TCPConn, src *net.TCPConn, breakConn *bool, wg *sync.WaitGroup) {
	buf := make([]byte, 4096)
	for {
		if *breakConn == false {
			src.SetReadDeadline(time.Now().Add(5 * time.Second))
			if n, err := src.Read(buf); err == nil {
				if _, err := des.Write(buf[:n]); err != nil {
					break
				}
			} else {
				if opError, ok := err.(*net.OpError); ok {
					if !opError.Timeout() {
						break
					} else {
						continue
					}
				}
				break
			}
		} else {
			break
		}
	}
	*breakConn = true
	wg.Done()
	log.Println("Copy to exit.")
}

func Swap(inConn *net.TCPConn, outConn *net.TCPConn) {
	log.Println("Swap begin.")
	wg := new(sync.WaitGroup)
	defer func() {
		inConn.Close()
		outConn.Close()
		log.Println("Swap exit.")
	}()
	breakConn := false
	wg.Add(2)
	go Copyto(outConn, inConn, &breakConn, wg)
	go Copyto(inConn, outConn, &breakConn, wg)
	wg.Wait()
}

func DialTCP(address string) (*net.TCPConn, error) {
	var err error
	var conn *net.TCPConn = nil
	MaxTry := 5
	TCPAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	for ; MaxTry >= 0; {
		conn, err = net.DialTCP("tcp", nil, TCPAddr)
		if err != nil {
			MaxTry--
		} else {
			log.Printf("Connect to %s success.\n", address)
			break
		}
	}
	return conn, err
}

func main() {
	flag.Parse()

	remoteAddr = fmt.Sprintf("%s:%d", *remoteIP, *remotePort)
	proxyCtrlAddr = fmt.Sprintf("%s:%d", *proxyIP, *proxyCtrlPort)
	proxyDataAddr = fmt.Sprintf("%s:%d", *proxyIP, *proxyDataPort)

	clientCtrlConn := NewClientCtrlConn()
	for {
		proxyTCPAddr, err := net.ResolveTCPAddr("tcp", proxyCtrlAddr)
		if err != nil {
			log.Println("Cann't resolve proxy connpool address.")
			log.Fatal(err)
		}
		conn, err := net.DialTCP("tcp", nil, proxyTCPAddr)
		if err != nil {
			log.Println("Connect to proxy connpool failed.")
			log.Fatal(err)
		} else {
			log.Println("Connect to proxy connpool success.")
			clientCtrlConn.Init(conn)
			clientCtrlConn.Work()
			clientCtrlConn.CleanUp()
			log.Println("Reconnect to  proxy connpool.")
		}
	}
}
