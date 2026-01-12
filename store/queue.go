package store

import "cube/task"

type Queue[T any] interface {
	Enqueue(T)
	Dequeue() T
	Peek() T
	Len() int
}

type TaskQueue struct {
	vals []task.Task
}

var _ Queue[task.Task] = (*TaskQueue)(nil)

func (tq *TaskQueue) Enqueue(t task.Task) {
	tq.vals = append(tq.vals, t)
}

func (tq *TaskQueue) Dequeue() task.Task {
	tmp := tq.vals[0]
	tq.vals = tq.vals[1:]
	return tmp
}

func (tq *TaskQueue) Peek() task.Task {
	return tq.vals[0]
}

func (tq *TaskQueue) Len() int {
	return len(tq.vals)
}

type TaskEventQueue struct {
	vals []task.TaskEvent
}

var _ Queue[task.TaskEvent] = (*TaskEventQueue)(nil)

func (tq *TaskEventQueue) Enqueue(t task.TaskEvent) {
	tq.vals = append(tq.vals, t)
}

func (tq *TaskEventQueue) Dequeue() task.TaskEvent {
	tmp := tq.vals[0]
	tq.vals = tq.vals[1:]
	return tmp
}

func (tq *TaskEventQueue) Peek() task.TaskEvent {
	return tq.vals[0]
}

func (tq *TaskEventQueue) Len() int {
	return len(tq.vals)
}
