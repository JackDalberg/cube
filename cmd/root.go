package cmd

import (
	"fmt"
	"os"
	"strings"
)

func Execute() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		os.Exit(1)
	}
	fmt.Println(os.Args)
	subArgs := os.Args[2:]
	switch strings.ToLower(os.Args[1]) {
	case "worker":
		_worker(subArgs)
	case "manager":
		_manager(subArgs)
	case "run":
		_run(subArgs)
	case "node":
		_node(subArgs)
	case "status":
		_status(subArgs)
	case "stop":
		_stop(subArgs)
	default:

	}
}
