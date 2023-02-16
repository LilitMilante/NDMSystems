package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	chanBuf = 8192
)

func main() {
	http.HandleFunc("/", Queue)

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}

type Queues struct {
	mux sync.RWMutex
	m   map[string]chan string
}

var queues = &Queues{
	// init topics
	m: map[string]chan string{
		"pet":  make(chan string, chanBuf),
		"role": make(chan string, chanBuf),
	},
}

func (q *Queues) add(queue string, message string) {
	q.mux.Lock()
	defer q.mux.Unlock()

	_, ok := q.m[queue]
	if !ok {
		q.m[queue] = make(chan string, chanBuf)
	}

	q.m[queue] <- message
}

func (q *Queues) get(queue string) <-chan string {
	q.mux.RLock()
	defer q.mux.RUnlock()

	return queues.m[queue]
}

func Queue(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		PUTQueue(w, r)
	case http.MethodGet:
		GETQueue(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func PUTQueue(w http.ResponseWriter, r *http.Request) {
	// get queues name
	nameQueue := r.URL.Path
	queue := nameQueue[1:]

	// get message
	message := r.URL.Query().Get("v")
	if message == "" {
		http.Error(w, "empty message", http.StatusBadRequest)
		return
	}

	queues.add(queue, message)
	// default response is 200
}

func GETQueue(w http.ResponseWriter, r *http.Request) {
	nameQueue := r.URL.Path
	queue := nameQueue[1:]
	if queue == "" {
		http.Error(w, "empty path", http.StatusBadRequest)
		return
	}

	timeout := r.URL.Query().Get("timeout")
	if timeout == "" {
		timeout = "5"
	}

	timeoutSec, err := strconv.Atoi(timeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	timeoutCtx, cancel := context.WithTimeout(r.Context(), time.Second*time.Duration(timeoutSec))
	defer cancel()

	select {
	case msg := <-queues.get(queue):
		fmt.Fprint(w, msg)
	case <-timeoutCtx.Done():
		w.WriteHeader(http.StatusNotFound)
	}
}
