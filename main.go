package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

type Job struct {
	Name   string
	Delay  time.Duration
	Number int
}

type Worker struct {
	Id         int
	JobQueue   chan Job
	WorkerPool chan chan Job
	QuitChan   chan bool
}

type Dispatcher struct {
	WorkerPool chan chan Job
	MaxWorkers int
	JobQueue   chan Job
}

// constructor
func NewWorker(id int, workerPool chan chan Job) *Worker {
	return &Worker{
		Id:         id,
		JobQueue:   make(chan Job),
		WorkerPool: workerPool,
		QuitChan:   make(chan bool),
	}
}

// Create a method for the worker using the receiver functions
func (w Worker) Start() {
	go func() {
		// Indefined iteration
		for {
			w.WorkerPool <- w.JobQueue // Reading from w.JobQueue
			select {                   // multiplexation
			case job := <-w.JobQueue:
				fmt.Printf("Worker with id: %d has started\n", w.Id)
				fib := Fibonacci(job.Number)
				time.Sleep(job.Delay)
				fmt.Printf("Worker with Id: %d has ended with fibonacci result %d\n", w.Id, fib)
			case <-w.QuitChan:
				fmt.Printf("Worker with id %d Stopped \n", w.Id)
			}
		}
	}()
}

func (w Worker) Stop() {
	go func() {
		w.QuitChan <- true
	}()
}

func Fibonacci(n int) int {
	if n <= 1 {
		return n
	}
	return Fibonacci(n-1) + Fibonacci(n-2)
}

// constructor for dispatecher
func NewDispatcher(jobQueue chan Job, maxWorkers int) *Dispatcher {
	worker := make(chan chan Job, maxWorkers)
	return &Dispatcher{
		WorkerPool: worker,
		MaxWorkers: maxWorkers,
		JobQueue:   jobQueue,
	}
}

func (d *Dispatcher) Dispatch() {
	for {
		select {
		case job := <-d.JobQueue:
			go func() {
				workerJobQueue := <-d.WorkerPool
				workerJobQueue <- job
			}()
		}
	}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.WorkerPool)
		worker.Start()
	}
	go d.Dispatch()
}

// the following function handles http requests
func RequestHandler(w http.ResponseWriter, r *http.Request, jobQueue chan Job) {

	switch r.Method {
	case http.MethodPost:
		delay, err := time.ParseDuration(r.FormValue("delay"))
		if err != nil {
			http.Error(w, "Invalid delay", http.StatusBadRequest)
			return
		}
		value, err := strconv.Atoi(r.FormValue("value"))
		if err != nil {
			http.Error(w, "Invalid value", http.StatusBadRequest)
			return
		}
		name := r.FormValue("name")
		if name == "" {
			http.Error(w, "Invalid name", http.StatusBadRequest)
			return
		}
		job := Job{Name: name, Delay: delay, Number: value}
		jobQueue <- job
		w.WriteHeader(http.StatusCreated)
	default:
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

}

func main() {
	const (
		maxWorkers       = 4
		maxQueueSizeJobs = 20
		port             = ":8081"
	)
	jobQueue := make(chan Job, maxQueueSizeJobs) // Buffered channel
	dispatcher := NewDispatcher(jobQueue, maxWorkers)
	dispatcher.Run()
	// http://localhost:8081/fib
	http.HandleFunc("/fib", func(rw http.ResponseWriter, r *http.Request) {
		RequestHandler(rw, r, jobQueue)
	})
	log.Fatal(http.ListenAndServe(port, nil))

}
