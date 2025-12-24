package main

import (
	"cube/manager"
	"cube/task"
	"cube/worker"
	"fmt"
	"os"
	"strconv"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

func main() {
	// Ports 5556, 5557, 5558 in use
	whost := os.Getenv("CUBE_WORKER_HOST")
	wport, _ := strconv.Atoi(os.Getenv("CUBE_WORKER_PORT"))
	// Port 5555 in use
	mhost := os.Getenv("CUBE_MANAGER_HOST")
	mport, _ := strconv.Atoi(os.Getenv("CUBE_MANAGER_PORT"))

	fmt.Println("Starting cube workers")

	w1 := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi1 := worker.Api{Address: whost, Port: wport, Worker: &w1}
	w2 := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi2 := worker.Api{Address: whost, Port: wport + 1, Worker: &w2}
	w3 := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi3 := worker.Api{Address: whost, Port: wport + 2, Worker: &w3}

	// go w1.CollectStats()
	go w1.RunTasks()
	go w1.UpdateTasks()
	go wapi1.Start()

	go w2.RunTasks()
	go w2.UpdateTasks()
	go wapi2.Start()

	go w3.RunTasks()
	go w3.UpdateTasks()
	go wapi3.Start()

	fmt.Println("Starting cube manager")

	workers := []string{
		fmt.Sprintf("%s:%d", whost, wport),
		fmt.Sprintf("%s:%d", whost, wport+1),
		fmt.Sprintf("%s:%d", whost, wport+2),
	}
	m := manager.New(workers, "epvm")
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	mapi.Start()

	// for i := range 3 {
	// 	t := task.Task{
	// 		ID:    uuid.New(),
	// 		Name:  fmt.Sprintf("test-container-%d", i),
	// 		State: task.Scheduled,
	// 		Image: "strm/helloworld-http",
	// 	}
	// 	te := task.TaskEvent{
	// 		ID:    uuid.New(),
	// 		State: task.Running,
	// 		Task:  t,
	// 	}
	// 	m.AddTask(te)
	// 	m.SendWork()
	// }

	// go func() {
	// 	for {
	// 		fmt.Printf("[Manager] Updating tasks from %d workers\n", len(m.Workers))
	// 		m.UpdateTasks()
	// 		time.Sleep(15 * time.Second)
	// 	}
	// }()

	// for {
	// 	for _, t := range m.TaskDb {
	// 		fmt.Printf("[Manager] Task: id: %s, state: %d\n", t.ID, t.State)
	// 		time.Sleep(15 * time.Second)
	// 	}
	// }
}
