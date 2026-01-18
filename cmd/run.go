package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	flag "github.com/spf13/pflag"
)

func _run(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	manager := fs.StringP("manager", "m", "localhost:5555", "Manager to talk to.")
	filename := fs.StringP("filename", "f", "task.json", "Task specification file")
	fs.Parse(args)

	filePath, err := filepath.Abs(*filename)
	if err != nil {
		log.Fatal(err)
	}
	_, err = os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) {
		log.Fatalf("File %s does not exist.", filePath)
	}

	log.Printf("Using manager: %v\n", *manager)
	log.Printf("Using file: %v\n", filePath)

	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Unable to read file: %v", filePath)
	}
	url := fmt.Sprintf("http://%s/tasks", *manager)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		log.Fatalf("Error sending request: %v", resp.StatusCode)
	}
	log.Printf("Successfully sent task %s to manager", *filename)
}
