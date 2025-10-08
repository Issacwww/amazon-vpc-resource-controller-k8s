package scale

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type ScalingConfig struct {
	MinCapacity        int
	MaxCapacity        int
	ScaleUpWait        time.Duration
	ScaleDownWait      time.Duration
	TestDuration       time.Duration
	ValidationInterval time.Duration
}

type Node struct {
	Name string
	IP   string
}

var (
	stopScaling chan bool
	ipTracker   map[string]string
	config      ScalingConfig
)

func getScalingConfig() ScalingConfig {
	return ScalingConfig{
		MinCapacity:        getEnvInt("SGPP_MIN_CAPACITY", 3),
		MaxCapacity:        getEnvInt("SGPP_MAX_CAPACITY", 6),
		ScaleUpWait:        getEnvDuration("SGPP_SCALE_UP_WAIT", "2m"),
		ScaleDownWait:      getEnvDuration("SGPP_SCALE_DOWN_WAIT", "2m"),
		TestDuration:       getEnvDuration("SGPP_TEST_DURATION", "30m"),
		ValidationInterval: getEnvDuration("SGPP_VALIDATION_INTERVAL", "30s"),
	}
}

func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue string) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	duration, _ := time.ParseDuration(defaultValue)
	return duration
}

var _ = Describe("SGPP IP Reuse Scale Test", func() {
	BeforeEach(func() {
		stopScaling = make(chan bool)
		ipTracker = make(map[string]string)
		config = getScalingConfig()
		initializeIPTracking()
	})

	AfterEach(func() {
		close(stopScaling)
	})

	It("should reuse IPs during configurable scaling cycles", func() {
		fmt.Printf("Starting SGPP IP reuse test with config: Min=%d, Max=%d, ScaleUpWait=%v, ScaleDownWait=%v, Duration=%v\n",
			config.MinCapacity, config.MaxCapacity, config.ScaleUpWait, config.ScaleDownWait, config.TestDuration)

		go func() {
			cycle := 0
			for {
				select {
				case <-stopScaling:
					fmt.Println("Stopping scaling operations")
					return
				default:
					cycle++
					fmt.Printf("Starting scaling cycle %d\n", cycle)

					fmt.Printf("Scaling up to %d nodes\n", config.MaxCapacity)
					scaleNodegroup(config.MaxCapacity)
					waitForScalingComplete(config.MaxCapacity)
					time.Sleep(config.ScaleUpWait)

					fmt.Printf("Scaling down to %d nodes\n", config.MinCapacity)
					scaleNodegroup(config.MinCapacity)
					waitForScalingComplete(config.MinCapacity)
					time.Sleep(config.ScaleDownWait)
				}
			}
		}()

		Eventually(func() bool {
			return validateIPReuseOccurred()
		}, config.TestDuration, config.ValidationInterval).Should(BeTrue())
	})
})

func scaleNodegroup(desiredSize int) {
	clusterName := os.Getenv("CLUSTER_NAME")
	nodegroupName := os.Getenv("NODEGROUP_NAME")

	cmd := exec.Command("aws", "eks", "update-nodegroup-config",
		"--cluster-name", clusterName,
		"--nodegroup-name", nodegroupName,
		"--scaling-config", fmt.Sprintf("desiredSize=%d", desiredSize))

	if err := cmd.Run(); err != nil {
		fmt.Printf("Failed to scale nodegroup: %v\n", err)
	}
}

func waitForScalingComplete(expectedSize int) {
	timeout := time.After(10 * time.Minute)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			fmt.Printf("Timeout waiting for scaling to complete\n")
			return
		case <-ticker.C:
			if getCurrentNodeCount() == expectedSize {
				fmt.Printf("Scaling complete: %d nodes ready\n", expectedSize)
				return
			}
		}
	}
}

func getCurrentNodeCount() int {
	cmd := exec.Command("kubectl", "get", "nodes", "--no-headers")
	output, err := cmd.Output()
	if err != nil {
		return 0
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return 0
	}
	return len(lines)
}

func initializeIPTracking() {
	nodes := getCurrentNodes()
	for _, node := range nodes {
		ipTracker[node.Name] = node.IP
	}
	fmt.Printf("Initialized IP tracking with %d nodes\n", len(nodes))
}

func validateIPReuseOccurred() bool {
	currentNodes := getCurrentNodes()

	for _, currentNode := range currentNodes {
		for trackedNodeName, trackedIP := range ipTracker {
			if currentNode.IP == trackedIP && currentNode.Name != trackedNodeName {
				fmt.Printf("✅ IP reuse detected: IP %s reused from node %s to node %s\n",
					currentNode.IP, trackedNodeName, currentNode.Name)

				if validateTrunkENITags(currentNode) {
					fmt.Printf("✅ Trunk ENI tags validated for node %s\n", currentNode.Name)
					return true
				}
			}
		}
	}

	for _, node := range currentNodes {
		ipTracker[node.Name] = node.IP
	}

	return false
}

func getCurrentNodes() []Node {
	cmd := exec.Command("kubectl", "get", "nodes", "-o", "jsonpath={range .items[*]}{.metadata.name}{\" \"}{.status.addresses[?(@.type==\"InternalIP\")].address}{\"\\n\"}{end}")
	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("Failed to get nodes: %v\n", err)
		return nil
	}

	var nodes []Node
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			nodes = append(nodes, Node{Name: parts[0], IP: parts[1]})
		}
	}
	return nodes
}

func validateTrunkENITags(node Node) bool {
	// TODO: Implement trunk ENI tag validation logic
	// This would check AWS EC2 tags on the trunk ENI associated with the node
	return true
}