package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"net/http"
	"time"
)

var (
	pool *redis.Pool
)

func main() {
	pool = newPool("localhost:6379")
	http.HandleFunc("/hello", serveHome)
	http.ListenAndServe(":8000", nil)
}

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	conn := pool.Get()
	defer conn.Close()
	conn.Send("MULTI")
	conn.Send("LRANGE", "list", "0", "2")
	conn.Send("LTRIM", "list", "0", "2")
	conn.Send("EXEC")
	conn.Flush()
	v, err := conn.Receive()
	if err != nil {
		log.Println(err)
	}
	log.Println(v)
	w.Write([]byte("test"))
}
