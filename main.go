package main

import (
	"bytes"
	"database/sql"
	"flag"
	// "github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/redis.v4"
	"log"
	"runtime"
	// "strings"
	"sync"
	"time"
)

var client *redis.Client

type Task interface {
	Execute()
}

type Pool struct {
	mu sync.Mutex

	size  int
	tasks chan Task
	kill  chan struct{}
	wg    sync.WaitGroup
}

func NewPool(size int) *Pool {
	pool := &Pool{
		tasks: make(chan Task, 128),
		kill:  make(chan struct{}),
	}
	pool.Resize(size)
	return pool
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.tasks:
			if !ok {
				return
			}
			task.Execute()
		case <-p.kill:
			return
		}
	}
}

func (p *Pool) Resize(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.size < n {
		p.size++
		p.wg.Add(1)
		go p.worker()
	}
	for p.size > n {
		p.size--
		p.kill <- struct{}{}
	}
}

func (p *Pool) Close() {
	close(p.tasks)
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) Exec(task Task) {
	p.tasks <- task
}

type RedisTask struct {
	Index   int
	Command string
	Key     string
	Value   string
	MapData map[string]string
}

func (e RedisTask) Execute() {
	log.Println("executing:", e.Key, ",", e.Index)

	if e.Command == "SET" {
		err := client.Set(e.Key, e.Value, 0).Err()
		checkErr(err, "set error:")
	} else if e.Command == "SADD" {
		err := client.SAdd(e.Key, e.Value).Err()
		checkErr(err, "sadd error:")
	} else if e.Command == "HMSET" {
		err := client.HMSet(e.Key, e.MapData).Err()
		checkErr(err, "hmset error:")
	}
	// TODO: clean data
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	host := flag.String("s", "localhost:3306", "mysql server host and port ,eg localhost:3306")
	username := flag.String("u", "test", "username to login mysql")
	password := flag.String("p", "test", "password for mysql")
	database := flag.String("d", "test", "database you want to execute query")
	query := flag.String("q", "select 1;", "your query sql")
	ds := flag.String("ds", "key", "redis structure")
	PK := flag.String("pk", "Rkey", "the redis Key in the fields of mysql query result")

	redisHost := flag.String("rs", "localhost:6379", "redis host and port ,eg localhost:6379")
	redisPassword := flag.String("rp", "test", "redis password")

	poolSize := flag.Int("size", 10000, "redis pool size")

	flag.Parse()
	var buf bytes.Buffer = bytes.Buffer{}
	buf.WriteString(*username)
	buf.WriteString(":")
	buf.WriteString(*password)
	buf.WriteString("@tcp(")
	buf.WriteString(*host)
	buf.WriteString(")/")
	buf.WriteString(*database)

	db, err := sql.Open("mysql", buf.String())
	checkErr(err, "connect to mysql error !")
	defer db.Close()

	poolWorker := NewPool(*poolSize)

	// Execute the query
	rows, err := db.Query(*query)
	checkErr(err, "execute sql error!")

	// pool = newPool(*redisHost, *redisPassword, *poolSize)

	client = redis.NewClient(&redis.Options{
		Addr:     *redisHost,
		Password: *redisPassword, // no password set
		DB:       0,              // use default DB
	})

	pong, err := client.Ping().Result()
	checkErr(err, "redis client error:")
	log.Println(pong)

	columns, err := rows.Columns()
	checkErr(err, "get columns error!")

	length := len(columns)
	values := make([]sql.RawBytes, length)

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	count := 0
	for rows.Next() {
		count += 1
		err = rows.Scan(scanArgs...)
		checkErr(err, "scan error")

		var value string
		var key string

		var task RedisTask

		if *ds == "key" {
			key = getStringData(values[0])
			value = getStringData(values[1])
			if value != "" {
				task = RedisTask{
					Index:   count,
					Command: "SET",
					Key:     key,
					Value:   value,
				}
			}
		} else if *ds == "set" {
			key = getStringData(values[0])
			value = getStringData(values[1])
			if value != "" {
				task = RedisTask{
					Index:   count,
					Command: "SADD",
					Key:     key,
					Value:   value,
				}
			}
		} else if *ds == "hash" {
			key = getStringData(values[0])
			// args := redis.Args{}.Add(key)

			m := make(map[string]string)

			for i, col := range values {
				if col != nil && columns[i] != *PK {
					value = getStringData(col)
					m[columns[i]] = value
				}
			}
			task = RedisTask{
				Index:   count,
				Command: "HMSET",
				Key:     key,
				MapData: m,
			}
		}
		poolWorker.Exec(task)
	}
	if err = rows.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	poolWorker.Close()

	poolWorker.Wait()

	EndTime := time.Now().UnixNano() / int64(time.Millisecond)
	log.Println("======================================== executing time:", EndTime-startTime, " ms, totle:", count)
}

func getStringData(data sql.RawBytes) string {
	if data == nil {
		return ""
	}
	value := string(data)
	return clearBad(value)
}

func clearBad(str string) string {
	// str = strings.Trim(str, "`")
	// str = strings.Trim(str, "｀")
	// str = strings.Trim(str, "-")
	// str = strings.Trim(str, ".")
	// str = strings.Trim(str, " ")
	// str = strings.Trim(str, ";")
	// str = strings.Trim(str, ",")
	// str = strings.Trim(str, ":")
	// str = strings.Trim(str, ";")
	// str = strings.Trim(str, "'")
	// str = strings.Trim(str, "!")
	return str
}

func checkErr(err error, msg string) {
	if err != nil {
		log.Fatalln(msg, err)
	}
}
