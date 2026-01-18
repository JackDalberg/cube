package cmd

import (
	"cube/node"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"text/tabwriter"

	flag "github.com/spf13/pflag"
)

func _node(args []string) {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	manager := fs.StringP("manager", "m", "localhost:5555", "Manager to talk to.")
	fs.Parse(args)

	url := fmt.Sprintf("http://%s/nodes", *manager)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var nodes []*node.Node
	json.Unmarshal(body, &nodes)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', tabwriter.TabIndent)
	fmt.Fprintf(w, "Name\tMemory (MiB)\tDisk (GiB)\tRole\tTasks\t\n")
	for _, node := range nodes {
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%d\t\n", node.Name, node.Memory/1000, node.Disk/(1000*1000*1000), node.Role, node.TaskCount)
	}
	w.Flush()
}
