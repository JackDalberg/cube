package manager

import (
	"bytes"
	"cube/node"
	"cube/scheduler"
	"cube/task"
	"cube/worker"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[uuid.UUID]*task.Task
	EventDb       map[uuid.UUID]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
	WorkerNodes   []*node.Node
	Scheduler     scheduler.Scheduler
}

func New(workers []string, schedulerType string) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)
	var nodes []*node.Node
	for _, w := range workers {
		workerTaskMap[w] = []uuid.UUID{}

		nApi := fmt.Sprintf("http://%v", w)
		n := node.NewNode(w, nApi, "worker")
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case "roundrobin":
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	case "epvm":
		s = &scheduler.Epvm{Name: "epvm"}
	default:
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	}

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        make(map[uuid.UUID]*task.Task),
		EventDb:       make(map[uuid.UUID]*task.TaskEvent),
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: make(map[uuid.UUID]string),
		WorkerNodes:   nodes,
		Scheduler:     s,
	}
}

func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		msg := fmt.Sprintf("No available candidates match resource request for task %v", t.ID)
		return nil, errors.New(msg)
	}
	scores := m.Scheduler.Score(t, candidates)
	return m.Scheduler.Pick(scores, candidates), nil
}

func (m *Manager) UpdateTasks() {
	for {
		log.Println("Checking for tasks updates from workers")
		m.updateTasks()
		log.Println("Task updates completed, now sleeping 10s")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		log.Printf("Checking worker %v for task updates", worker)
		url := fmt.Sprintf("http://%s/tasks", worker)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", worker, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("Error sending request: %v\n", err)
			continue
		}

		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			log.Printf("Error unmarshalling tasks: %s\n", err)
			continue
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %v\n", t.ID)
			_, ok := m.TaskDb[t.ID]
			if !ok {
				log.Printf("Task with ID %s not found\n", t.ID)
				continue
			}

			m.TaskDb[t.ID].State = t.State
			m.TaskDb[t.ID].StartTime = t.StartTime
			m.TaskDb[t.ID].FinishTime = t.FinishTime
			m.TaskDb[t.ID].ContainerID = t.ContainerID
		}
	}
}

func (m *Manager) SendWork() {
	if m.Pending.Len() == 0 {
		log.Printf("No work in the queue\n")
		return
	}

	e := m.Pending.Dequeue()
	te := e.(task.TaskEvent)
	m.EventDb[te.ID] = &te
	log.Printf("Pulled %v off pending queue\n", te)

	taskWorker, ok := m.TaskWorkerMap[te.Task.ID]
	if ok {
		taskPersisted := m.TaskDb[te.Task.ID]
		if te.State == task.Completed && task.ValidStateTransition(taskPersisted.State, te.State) {
			m.stopTask(taskWorker, te.Task.ID.String())
			log.Printf("Invalid request: existing task %s is in state %v and cannot transition to the completed state (%v)\n", taskPersisted.ID.String(), taskPersisted.State, task.Completed)
			return
		}
	}

	t := te.Task
	w, err := m.SelectWorker(t)
	if err != nil {
		log.Printf("Error selecting worker for task %s: %v\n", t.ID, err)
		return
	}
	m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], te.Task.ID)
	m.TaskWorkerMap[t.ID] = w.Name

	t.State = task.Scheduled
	m.TaskDb[t.ID] = &t

	data, err := json.Marshal(te)
	if err != nil {
		log.Printf("Unable to marshal task object: %v\n", t)
	}

	url := fmt.Sprintf("http://%s/tasks", w.Name)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Error connecting to %v: %v\n", w, err)
		m.Pending.Enqueue(te)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			fmt.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		log.Printf("Response error (%d): %s", e.HTTPStatusCode, e.Message)
		return
	}

	t = task.Task{}
	err = d.Decode(&t)
	if err != nil {
		fmt.Printf("Error deocding response: %s\n", err.Error())
		return
	}
	log.Printf("%#v", t)
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

func (m *Manager) GetTasks() []*task.Task {
	var tasks []*task.Task
	for _, t := range m.TaskDb {
		tasks = append(tasks, t)
	}
	return tasks
}

func (m *Manager) ProcessTasks() {
	for {
		log.Println("Processing any tasks in the queue")
		m.SendWork()
		log.Println("Sleeping for 10s")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) DoHealthChecks() {
	for {
		log.Println("Performing task health check")
		m.doHealthChecks()
		log.Println("Task health checks completed, sleeping 60s")
		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {
		if t.RestartCount >= 3 {
			continue
		}
		switch t.State {
		case task.Running:
			err := m.checkTaskHealth(*t)
			if err != nil {
				m.restartTask(t)
			}
		case task.Failed:
			m.restartTask(t)
		}
	}
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	log.Printf("Calling health check for task %s: %s", t.ID, t.HealthCheck)

	w := m.TaskWorkerMap[t.ID]

	hostPort := getHostPort(t.HostPorts)
	if hostPort == nil {
		log.Printf("Have not collected host ports from %v yet. Skipping for now\n", t.ID)
		return nil
	}

	worker := strings.Split(w, ":")
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)
	resp, err := http.Get(url)
	if err != nil {
		msg := fmt.Sprintf("Error connecting to health check %s", url)
		log.Println(msg)
		return errors.New(msg)
	}
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error health check for task %s did not return 200\n", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}
	log.Printf("Task %s health check response: %v\n", t.ID, resp.StatusCode)
	return nil
}

func (m *Manager) restartTask(t *task.Task) {
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++
	m.TaskDb[t.ID] = t

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}
	data, err := json.Marshal(te)
	if err != nil {
		log.Printf("Unable to marshal task object: %v", t)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Error connecting to %v: %v", w, err)
		m.Pending.Enqueue(t)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		var e worker.ErrResponse
		err := d.Decode(&e)
		if err != nil {
			fmt.Printf("Error decoding response: %s\n", err)
			return
		}
		log.Printf("Response error (%d): %v", e.HTTPStatusCode, e.Message)
		return
	}

	var newTask task.Task
	err = d.Decode(&newTask)
	if err != nil {
		fmt.Printf("Error decoding response: %s\n", err)
		return
	}
	log.Printf("%#v\n", t)
}

func getHostPort(ports nat.PortMap) *string {
	for k := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}

func (m *Manager) stopTask(worker, taskID string) {
	var client http.Client
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("Error creating request to delete task %s: %v\n", taskID, err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error connecting to worker at %s: %v\n", url, err)
		return
	}

	if resp.StatusCode != http.StatusNoContent {
		log.Printf("error sending request: %v\n", err)
		return
	}

	log.Printf("task %s has been scheduled to be stopped", taskID)
}
