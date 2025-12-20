package worker

import (
	"cube/task"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/google/uuid"
)

type ErrResponse struct {
	HTTPStatusCode int
	Message        string
}

type Api struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *http.ServeMux
}

func (a *Api) initRouter() {
	a.Router = http.NewServeMux()
	a.Router.HandleFunc("GET /tasks", a.GetTaskHandler)
	a.Router.HandleFunc("POST /tasks", a.StartTaskHandler)
	a.Router.HandleFunc("DELETE /tasks/{taskID}", a.StopTaskHandler)
}

func (a *Api) Start() {
	a.initRouter()
	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
}

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	var te task.TaskEvent
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Print(msg)
		w.WriteHeader(400)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}
	a.Worker.AddTask(te.Task)
	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(te.Task)
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("taskID")
	if taskID == "" {
		log.Printf("No taskID passed in request\n")
		w.WriteHeader(400)
		return
	}

	tID, _ := uuid.Parse(taskID)
	taskToStop, ok := a.Worker.Db[tID]
	if !ok {
		log.Printf("No task with ID %v found\n", tID)
		w.WriteHeader(404)
		return
	}
	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)
	log.Printf("added task %v to stop container %v\n", taskToStop.ID, taskToStop.ContainerID)
	w.WriteHeader(204)
}

func (a *Api) GetTaskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}
