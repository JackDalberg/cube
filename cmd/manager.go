package cmd

import (
	"cube/manager"
	"log"

	flag "github.com/spf13/pflag"
)

func _manager(args []string) {
	fs := flag.NewFlagSet("manager", flag.ExitOnError)
	host := fs.StringP("host", "h", "0.0.0.0", "Hostname or IP address.")
	port := fs.IntP("port", "p", 5555, "Port on which to listen.")
	workers := fs.StringSliceP("workers", "w", []string{"localhost:5556"}, "List of workers on which the manager will schedule tasks.")
	scheduler := fs.StringP("scheduler", "s", "epvm", "Name of scheduler to use.")
	dbtype := fs.StringP("dbtype", "d", "memory", "Type of datastore to use for events and tasks (\"memory\" or \"bolt\")")
	fs.Parse(args)

	log.Println("Starting manager.")
	m := manager.New(*workers, *scheduler, *dbtype)
	api := manager.Api{Address: *host, Port: *port, Manager: m}
	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()
	// go m.UpdateNodeStats()
	log.Printf("Starting manager API on http://%s:%d", *host, *port)
	api.Start()
}
