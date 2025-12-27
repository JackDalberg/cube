package store

import (
	"cube/task"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
)

type Store interface {
	Put(key string, value any) error
	Get(key string) (any, error)
	List() (any, error)
	Count() (int, error)
}

type InMemoryTaskStore struct {
	Db map[string]*task.Task
}

var _ Store = (*InMemoryTaskStore)(nil)

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		Db: make(map[string]*task.Task),
	}
}

func (i *InMemoryTaskStore) Put(key string, value any) error {
	t, ok := value.(*task.Task)
	if !ok {
		return fmt.Errorf("value %v is not a task.Task type", value)
	}
	i.Db[key] = t
	return nil
}

func (i *InMemoryTaskStore) Get(key string) (any, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task.Task with key %v does not exist", key)
	}
	return t, nil
}

func (i *InMemoryTaskStore) List() (any, error) {
	var tasks []*task.Task
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	return len(i.Db), nil
}

type InMemoryTaskEventStore struct {
	Db map[string]*task.TaskEvent
}

var _ Store = (*InMemoryTaskEventStore)(nil)

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		Db: make(map[string]*task.TaskEvent),
	}
}

func (i *InMemoryTaskEventStore) Put(key string, value any) error {
	t, ok := value.(*task.TaskEvent)
	if !ok {
		return fmt.Errorf("value %v is not a task.TaskEvent type", value)
	}
	i.Db[key] = t
	return nil
}

func (i *InMemoryTaskEventStore) Get(key string) (any, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task.TaskEvent with key %v does not exist", key)
	}
	return t, nil
}

func (i *InMemoryTaskEventStore) List() (any, error) {
	var tasks []*task.TaskEvent
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (i *InMemoryTaskEventStore) Count() (int, error) {
	return len(i.Db), nil
}

type BoltTaskStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

var _ Store = (*BoltTaskStore)(nil)

func NewBoltTaskStore(file, bucket string, mode os.FileMode) (*BoltTaskStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open %v: %v", file, err)
	}
	b := BoltTaskStore{
		Db:       db,
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
	}

	err = b.CreateBucket()
	if err != nil {
		log.Printf("bucket %v already exists, will reuse it", b.Bucket)
	}
	return &b, nil
}

func (b *BoltTaskStore) Put(key string, value any) error {
	return b.Db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Bucket))
		marshalled, err := json.Marshal(value.(*task.Task))
		if err != nil {
			return err
		}
		err = bucket.Put([]byte(key), marshalled)
		return err
	})
}

func (b *BoltTaskStore) Get(key string) (any, error) {
	var task task.Task
	err := b.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Bucket))
		result := bucket.Get([]byte(key))
		if result == nil {
			return fmt.Errorf("task %v not found", key)
		}
		err := json.Unmarshal(result, &task)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (b *BoltTaskStore) List() (any, error) {
	var tasks []*task.Task
	err := b.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Bucket))
		bucket.ForEach(func(k, v []byte) error {
			var task task.Task
			err := json.Unmarshal(v, &task)
			if err != nil {
				return err
			}
			tasks = append(tasks, &task)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (b *BoltTaskStore) Count() (int, error) {
	taskCount := 0
	err := b.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Bucket))
		bucket.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}
	return taskCount, nil
}

func (b *BoltTaskStore) Close() {
	b.Db.Close()
}

func (b *BoltTaskStore) CreateBucket() error {
	return b.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(b.Bucket))
		if err != nil {
			return fmt.Errorf("create bucket %s: %s", b.Bucket, err)
		}
		return nil
	})
}

type BoltTaskEventStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

var _ Store = (*BoltTaskEventStore)(nil)

func NewBoltTaskEventStore(file, bucket string, mode os.FileMode) (*BoltTaskEventStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open %v: %v", file, err)
	}
	b := BoltTaskEventStore{
		Db:       db,
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
	}

	err = b.CreateBucket()
	if err != nil {
		log.Printf("bucket %v already exists, will reuse it", b.Bucket)
	}
	return &b, nil
}

func (b *BoltTaskEventStore) Put(key string, value any) error {
	return b.Db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Bucket))
		marshalled, err := json.Marshal(value.(*task.TaskEvent))
		if err != nil {
			return err
		}
		err = bucket.Put([]byte(key), marshalled)
		return err
	})
}

func (b *BoltTaskEventStore) Get(key string) (any, error) {
	var event task.TaskEvent
	err := b.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Bucket))
		result := bucket.Get([]byte(key))
		if result == nil {
			return fmt.Errorf("event %v not found", key)
		}
		err := json.Unmarshal(result, &event)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &event, nil
}

func (b *BoltTaskEventStore) List() (any, error) {
	var events []*task.TaskEvent
	err := b.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Bucket))
		bucket.ForEach(func(k, v []byte) error {
			var event task.TaskEvent
			err := json.Unmarshal(v, &event)
			if err != nil {
				return err
			}
			events = append(events, &event)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (b *BoltTaskEventStore) Count() (int, error) {
	taskCount := 0
	err := b.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Bucket))
		bucket.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}
	return taskCount, nil
}

func (b *BoltTaskEventStore) Close() {
	b.Db.Close()
}

func (b *BoltTaskEventStore) CreateBucket() error {
	return b.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(b.Bucket))
		if err != nil {
			return fmt.Errorf("create bucket %s: %s", b.Bucket, err)
		}
		return nil
	})
}
