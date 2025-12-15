package worker

import (
	"cube/task"
	"fmt"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
}

func (w *Worker) CollectStats() {
	fmt.Println("TODO: Collect stats")
}

func (w *Worker) RunTask() {
	fmt.Println("TODO: Run task")
}

func (w *Worker) StartTask() {
	fmt.Println("TODO: Start task")
}

func (w *Worker) StopTask() {
	fmt.Println("TODO: Stop task")
}
