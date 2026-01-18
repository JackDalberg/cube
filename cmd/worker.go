package cmd

import (
	"cube/worker"
	"fmt"
	"log"

	"github.com/google/uuid"
	flag "github.com/spf13/pflag"
)

func _worker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	host := fs.StringP("host", "H", "0.0.0.0", "Hostname or IP address.")
	port := fs.IntP("port", "p", 5556, "Port on which to listen.")
	name := fs.StringP("name", "n", fmt.Sprintf("worker-%s", uuid.New().String()), "Name of the worker.")
	dbtype := fs.StringP("dbtype", "d", "memory", "Type of datastore to use for tasks (\"memory\" or \"bolt\").")
	fs.Parse(args)

	log.Printf("Starting worker.")
	w := worker.New(*name, *dbtype)
	api := worker.Api{Address: *host, Port: *port, Worker: w}
	go w.RunTasks()
	go w.CollectStats()
	go w.UpdateTasks()
	log.Printf("starting worker API on http://%s:%d", *host, *port)
	api.Start()
}
