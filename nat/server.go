package main

import (
	"flag"
	"net"
	"log"
	"fmt"
	"time"
	"sync"
	"bufio"
)

var ServerCtrlPort = flag.Int("port", 8080, "nat port")
var InDataPort = flag.Int("iport", 8080, "bridge port")
var OutDataPort = flag.Int("oport", 8080, "client port")

var CtrlAddr string
var InDataAddr string
var OutDataAddr string

type ServeCtrlConn struct {
	conn       *net.TCPConn
	Alive      bool
	Mutex      sync.Mutex
	CtrlRWChan chan string
	ConnChan   chan *net.TCPConn
}

func NewServerCtrlConn() *ServeCtrlConn {
	c := new(ServeCtrlConn)
	return c
}

func (c *ServeCtrlConn) Init(conn *net.TCPConn) {
	c.conn = conn
	c.Alive = true
	c.CtrlRWChan = make(chan string)
	c.ConnChan = make(chan *net.TCPConn)
}

func (c *ServeCtrlConn) Dead() {
	c.Mutex.Lock()
	c.Alive = false
	c.Mutex.Unlock()
	log.Println("Ctrl Connection Dead.")
}

func (c *ServeCtrlConn) CleanUp() {
	log.Println("Serve ctrl connection clean up.")
	c.conn.Close()
	close(c.CtrlRWChan)
	close(c.ConnChan)
}

func (c *ServeCtrlConn) Work() {
	var wg sync.WaitGroup
	wg.Add(2)
	go c.CtrlConnWrite(&wg)
	go c.CtrlConnRead(&wg)
	wg.Wait()
	log.Println("Done.")
}

func (c *ServeCtrlConn) CtrlConnRead(wg *sync.WaitGroup) {
	connReader := bufio.NewReader(c.conn)
forloop:
	for {
		c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		if readBytes, err := connReader.ReadBytes('\n'); err == nil {
			switch string(readBytes[:len(readBytes)-1]) {
			case "HB":
				log.Println("Recieve heart beat package.")
				c.CtrlRWChan <- "HB"
				break
			case "FAIL":
				c.ConnChan <- nil
			}
		} else {
			c.Dead()
			break forloop
		}
	}
	log.Println("Ctrl Read exit")
	wg.Done()
}

func (c *ServeCtrlConn) CtrlConnWrite(wg *sync.WaitGroup) {
	CheckTicker := time.NewTicker(1 * time.Second)
forloop:
	for {
		select {
		case <-CheckTicker.C:
			if !c.Alive {
				break forloop
			}
			break
		case toSend := <-c.CtrlRWChan:
			toSendBuf := append([]byte(toSend), '\n')
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := c.conn.Write(toSendBuf); err == nil {
				log.Println("Send heart beat response package.")
			} else {
				c.Dead()
				break forloop
			}
		}
	}
	log.Println("Ctrl Write exit")
	wg.Done()
}

func InServe(c *ServeCtrlConn) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", InDataAddr)
	if err != nil {
		log.Fatal("Resolve inner connpool address error")
		return
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	defer listener.Close()
	if err != nil {
		log.Fatal("Start inner listener error.")
		return
	}
	log.Println("Inner connpool started.")
	for {
		innerConn, err := listener.AcceptTCP()
		if err != nil {
			log.Println("Inner connection establish error.")
			log.Println(err)
		} else {
			if c.Alive {
				c.ConnChan <- innerConn
			} else {
				innerConn.Close()
			}
		}
	}
	log.Println("InServer exit.")
}

func OutServe(c *ServeCtrlConn) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", OutDataAddr)
	if err != nil {
		log.Fatal("Resolve outer connpool address error")
		return
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	defer listener.Close()
	if err != nil {
		log.Fatal("Start outer listener error.")
		return
	}
	log.Println("Outer Server started.")

	for {
		if outerConn, err := listener.AcceptTCP(); err == nil {
			log.Println("Request new inner connection.")
			if c.Alive {
				c.CtrlRWChan <- "NEW"
				timer := time.NewTimer(1 * time.Minute)
				select {
				case <-timer.C:
					log.Println("New inner connection timeout, close out connection.")
					outerConn.Close()
				case innerConn := <-c.ConnChan:
					if innerConn != nil {
						log.Println("Establish new proxy connection sucess.")
						go Swap(outerConn, innerConn)
					} else {
						log.Println("Establish new proxy connection failed.")
					}

				}
			} else {
				log.Println("Ctrl Connection not alive,can't establish new proxy.")
				outerConn.Close()
			}
		} else {
			log.Println("Outer connection establish error.")
		}
	}
	log.Println("OutServer exit.")
}

func CopyTo(des *net.TCPConn, src *net.TCPConn, breakConn *bool, wg *sync.WaitGroup) {
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
	go CopyTo(outConn, inConn, &breakConn, wg)
	go CopyTo(inConn, outConn, &breakConn, wg)
	wg.Wait()
}

func Serve() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", CtrlAddr)
	if err != nil {
		log.Fatal("Resolve nat address error")
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	defer listener.Close()
	if err != nil {
		log.Fatal("Listener start error.")
	}
	log.Println("Ctrl Server started.")
	serveCtrlConn := NewServerCtrlConn()
	go InServe(serveCtrlConn)
	go OutServe(serveCtrlConn)
	for {
		log.Println("Ctrl Server listening...")
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Println("Can not establish ctrl connection.")
			log.Fatal(err)
		} else {
			log.Printf("Conection from %s.\n", conn.RemoteAddr())
			serveCtrlConn.Init(conn)
			serveCtrlConn.Work()
			serveCtrlConn.CleanUp()
		}
	}
}

func main() {
	flag.Parse()
	CtrlAddr = fmt.Sprintf("0.0.0.0:%d", *ServerCtrlPort)
	InDataAddr = fmt.Sprintf("0.0.0.0:%d", *InDataPort)
	OutDataAddr = fmt.Sprintf("0.0.0.0:%d", *OutDataPort)
	Serve()
}
