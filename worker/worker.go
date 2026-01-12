package worker

import (
	"cube/stats"
	"cube/store"
	"cube/task"
	"errors"
	"fmt"
	"log"
	"time"
)

type Worker struct {
	Name      string
	Queue     store.Queue[task.Task]
	Db        store.Store[task.Task]
	TaskCount int
	Stats     *stats.Stats
}

func New(name, taskDBType string) *Worker {
	var s store.Store[task.Task]
	var err error
	switch taskDBType {
	case "memory":
		s = store.NewInMemoryTaskStore()
	case "bolt":
		filename := fmt.Sprintf("%s_tasks.db", name)
		s, err = store.NewBoltTaskStore(filename, "tasks", 0600)
	default:
		s = store.NewInMemoryTaskStore()
	}
	if err != nil {
		log.Fatalf("Unable to create task store for worker %s: %v", name, err)
	}
	return &Worker{
		Name:  name,
		Db:    s,
		Queue: &store.TaskQueue{},
	}
}

// Runs in its own goroutine.
func (w *Worker) CollectStats() {
	for {
		log.Printf("Collecting stats")
		w.Stats = stats.GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(15 * time.Second)
	}
}

// Runs in its own goroutine.
func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			result := w.runTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Print("No tasks to process rn\n")
		}
		log.Print("Sleeping for 10 secs\n")
		time.Sleep(10 * time.Second)
	}
}

// TODO(jack): Rewrite to be more intuitive to reason through.
func (w *Worker) runTask() task.DockerResult {
	if w.Queue.Len() == 0 {
		log.Printf("No tasks in the queue\n")
		return task.DockerResult{Error: nil}
	}
	taskQueued := w.Queue.Dequeue()
	err := w.Db.Put(taskQueued.ID.String(), &taskQueued)
	if err != nil {
		log.Printf("Unable to store task with ID %v\n", taskQueued.ID.String())
		return task.DockerResult{Error: err}
	}

	taskPersisted, err := w.Db.Get(taskQueued.ID.String())
	if err != nil {
		log.Printf("Unable to find task with ID %v\n", taskQueued.ID.String())
		return task.DockerResult{Error: err}
	}

	if taskPersisted == nil {
		taskPersisted = &taskQueued
		w.Db.Put(taskQueued.ID.String(), &taskQueued)
	}

	// TODO(jack): As of now, we are seeing whether or not the task can transition to
	// its current state. Should be more clear with the flow here
	var result task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			result.Error = errors.New("unreachable state transition")
		}
	} else {
		err := fmt.Errorf("invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
		result.Error = err
	}
	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	w.Db.Put(t.ID.String(), &t)
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	result := d.Run()
	if result.Error != nil {
		log.Printf("Error running task %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
		return result
	}
	t.ContainerID = result.ContainerId
	t.State = task.Running
	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("Error stopping container %v: %v\n", t.ContainerID, result.Error)
		return result
	}
	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db.Put(t.ID.String(), &t)
	log.Printf("Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)
	return result
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) GetTasks() []task.Task {
	var allTasks []task.Task
	taskList, err := w.Db.List()
	if err != nil {
		log.Printf("Error getting task list from taskDB: %v\n", err)
		return nil
	}
	for _, v := range taskList {
		allTasks = append(allTasks, *v)
	}
	return allTasks
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

// Runs in its own goroutine.
func (w *Worker) UpdateTasks() {
	for {
		log.Println("Checking status of tasks")
		w.updateTasks()
		log.Println("Task updates completed, sleeping for 15s")
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	taskList, err := w.Db.List()
	if err != nil {
		log.Printf("Error getting task list from taskDB: %v\n", err)
		return
	}
	for _, t := range taskList {
		if t.State != task.Running {
			continue
		}
		resp := w.InspectTask(*t)
		if resp.Error != nil {
			fmt.Printf("Err: %v\n", resp.Error)
			continue
		}
		if resp.Container == nil {
			log.Printf("No container for running task %s\n", t.ID)
			t.State = task.Failed
			w.Db.Put(t.ID.String(), t)
			continue
		}
		if resp.Container.State.Status == "exited" {
			log.Printf("Container for task %s in non-running state %s", t.ID, resp.Container.State.Status)
			t.State = task.Failed
			w.Db.Put(t.ID.String(), t)
			continue
		}
		t.HostPorts = resp.Container.NetworkSettings.Ports
		w.Db.Put(t.ID.String(), t)
	}
}
