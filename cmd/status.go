package cmd

import (
	"cube/task"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/docker/go-units"
	flag "github.com/spf13/pflag"
)

func _status(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	manager := fs.StringP("manager", "m", "localhost:5555", "Manager to talk to.")
	fs.Parse(args)

	url := fmt.Sprintf("http://%s/tasks", *manager)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	var tasks []*task.Task
	err = json.Unmarshal(body, &tasks)
	if err != nil {
		log.Fatal(err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', tabwriter.TabIndent)
	fmt.Fprintln(w, "ID\tName\tCreated\tState\tContainerName\tImage\t")
	for _, task := range tasks {
		var start string
		if task.StartTime.IsZero() {
			start = fmt.Sprintf("%s ago", units.HumanDuration(time.Now().UTC().Sub(time.Now().UTC())))
		} else {
			start = fmt.Sprintf("%s ago", units.HumanDuration(time.Now().UTC().Sub(task.StartTime)))
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t\n", task.ID, task.Name, start, task.State, task.Name, task.Image)
	}
	w.Flush()
}
