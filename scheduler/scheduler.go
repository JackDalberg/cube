package scheduler

import (
	"cube/node"
	"cube/task"
	"log"
	"math"
	"time"
)

const (
	LIEB = 1.53960071783900203869
)

type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node
	Score(t task.Task, nodes []*node.Node) map[string]float64
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}

type RoundRobin struct {
	Name       string
	LastWorker int
}

var _ Scheduler = (*RoundRobin)(nil)

func (r *RoundRobin) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	return nodes
}

func (r *RoundRobin) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)
	r.LastWorker = (r.LastWorker + 1) % len(nodes)
	for idx, node := range nodes {
		nodeScores[node.Name] = 1.0
		if idx == r.LastWorker {
			nodeScores[node.Name] = 0.1
		}
	}
	return nodeScores
}

func (r *RoundRobin) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var bestNode *node.Node
	lowestScore := math.MaxFloat64
	for _, node := range candidates {
		if scores[node.Name] < lowestScore {
			lowestScore = scores[node.Name]
			bestNode = node
		}
	}
	return bestNode
}

type Epvm struct {
	Name string
}

var _ Scheduler = (*Epvm)(nil)

func (e *Epvm) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	var candidates []*node.Node
	for _, node := range nodes {
		if checkDisk(t, node.Disk-node.DiskAllocated) {
			candidates = append(candidates, node)
		}
	}
	return candidates
}

func (e *Epvm) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)
	maxJobs := 4.0

	for _, node := range nodes {
		stats, err := node.GetStats()
		if err != nil || stats == nil {
			log.Printf("Error getting stats for node %v: %v\n", node, err)
			continue
		}
		cpuUsage, err := calculateCpuUsage(node)
		if err != nil {
			log.Printf("Error calculating cpu usage for node %s: %v\n", node.Name, err)
			cpuUsage = 1.0
		}
		// This asumes the max load of any node is 80%
		cpuLoad := cpuUsage / math.Pow(2.0, 0.8)

		memPercentWithoutTask := stats.MemUsedPercentage()
		memPercentWithTask := memPercentWithoutTask - (float64(t.Memory) / float64(stats.MemTotalKb()))

		// The final cpuLoad term was not in the original EPVM but makes sense to spread work based on cpu load
		marginalCost := math.Pow(LIEB, memPercentWithTask) - math.Pow(LIEB, memPercentWithoutTask) +
			math.Pow(LIEB, float64(node.TaskCount+1)/maxJobs) - math.Pow(LIEB, float64(node.TaskCount)/maxJobs) +
			math.Pow(LIEB, cpuLoad)

		nodeScores[node.Name] = marginalCost
	}

	return nodeScores
}

func (e *Epvm) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	minCost := math.MaxFloat64
	var bestNode *node.Node
	for _, node := range candidates {
		if scores[node.Name] < minCost {
			minCost = scores[node.Name]
			bestNode = node
		}
	}
	return bestNode
}

func checkDisk(t task.Task, diskAvailable int) bool {
	return t.Disk <= diskAvailable
}

func calculateCpuUsage(node *node.Node) (float64, error) {
	stat1, err := node.GetStats()
	if err != nil {
		return -1.0, err
	}
	time.Sleep(3 * time.Second)
	stat2, err := node.GetStats()
	if err != nil {
		return -1.0, err
	}

	stat1Idle := stat1.CpuStats.Idle + stat1.CpuStats.IOWait
	stat2Idle := stat2.CpuStats.Idle + stat2.CpuStats.IOWait

	stat1NonIdle := stat1.CpuStats.User + stat1.CpuStats.Nice + stat1.CpuStats.System + stat1.CpuStats.IRQ + stat1.CpuStats.SoftIRQ + stat1.CpuStats.Steal
	stat2NonIdle := stat2.CpuStats.User + stat2.CpuStats.Nice + stat2.CpuStats.System + stat2.CpuStats.IRQ + stat2.CpuStats.SoftIRQ + stat2.CpuStats.Steal

	stat1Total := stat1Idle + stat1NonIdle
	stat2Total := stat2Idle + stat2NonIdle

	total := stat2Total - stat1Total
	idle := stat2Idle - stat1Idle

	var cpuPercentUsage float64
	if total == 0 && idle == 0 {
		cpuPercentUsage = 0.00
	} else {
		cpuPercentUsage = (float64(total) - float64(idle)) / float64(total)
	}
	return cpuPercentUsage, nil
}
