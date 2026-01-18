package main

import (
	"context"
	"cube/cmd"
	"cube/manager"
	"cube/worker"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	cmd.Execute()
	os.Exit(0)
	// Ports 5556, 5557, 5558 in use
	whost := os.Getenv("CUBE_WORKER_HOST")
	wport, _ := strconv.Atoi(os.Getenv("CUBE_WORKER_PORT"))
	// Port 5555 in use
	mhost := os.Getenv("CUBE_MANAGER_HOST")
	mport, _ := strconv.Atoi(os.Getenv("CUBE_MANAGER_PORT"))

	fmt.Println("Starting cube workers")

	w1 := worker.New("worker-1", "memory")
	// w1 := worker.New("worker-1", "bolt")
	wapi1 := worker.Api{Address: whost, Port: wport, Worker: w1}
	w2 := worker.New("worker-2", "memory")
	// w2 := worker.New("worker-2", "bolt")
	wapi2 := worker.Api{Address: whost, Port: wport + 1, Worker: w2}
	w3 := worker.New("worker-3", "memory")
	// w3 := worker.New("worker-3", "bolt")
	wapi3 := worker.Api{Address: whost, Port: wport + 2, Worker: w3}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go w1.CollectStats()
	go w1.RunTasks()
	go w1.UpdateTasks()
	go wapi1.Start()

	go w2.CollectStats()
	go w2.RunTasks()
	go w2.UpdateTasks()
	go wapi2.Start()

	go w3.CollectStats()
	go w3.RunTasks()
	go w3.UpdateTasks()
	go wapi3.Start()

	fmt.Println("Starting cube manager")

	workers := []string{
		fmt.Sprintf("%s:%d", whost, wport),
		fmt.Sprintf("%s:%d", whost, wport+1),
		fmt.Sprintf("%s:%d", whost, wport+2),
	}
	m := manager.New(workers, "epvm", "memory")
	// m := manager.New(workers, "epvm", "bolt")
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	go mapi.Start()

	<-ctx.Done()
	fmt.Println("\rCancelled program, shutting down gracefully now")
}
