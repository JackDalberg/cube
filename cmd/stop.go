package cmd

import (
	"fmt"
	"log"
	"net/http"

	flag "github.com/spf13/pflag"
)

func _stop(args []string) {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	manager := fs.StringP("manager", "m", "localhost:5555", "Manager to talk to.")
	task := fs.StringP("task", "t", "", "Task to stop. If none is provided, will do nothing.")
	fs.Parse(args)

	if *task == "" {
		log.Fatal("Must provide task to stop.\n")
	}

	url := fmt.Sprintf("http://%s/tasks/%s", *manager, *task)
	client := http.DefaultClient
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Fatalf("Error creating request %v: %v", url, err)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error connecting to %v: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		log.Fatalf("Error sending request: %v", err)
	}
	log.Printf("Task %v has been stopped.\n", *task)
}
