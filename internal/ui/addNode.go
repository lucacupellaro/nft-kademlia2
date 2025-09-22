package ui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

func AddNode(ctx context.Context, nodeName, seederAddr, hostPort string) error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	defer cli.Close()

	// Path host per il bind mount: deve esistere
	hostDataPath, err := filepath.Abs(filepath.Join("./data", nodeName))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(hostDataPath, 0o755); err != nil {
		return err
	}

	// Verifica che la network di Compose esista
	if _, err := cli.NetworkInspect(ctx, "kademlia-nft_kadnet", types.NetworkInspectOptions{}); err != nil {
		return fmt.Errorf("rete 'kademlia-nft_kadnet' non trovata: %w", err)
	}

	port := nat.Port("8000/tcp")

	config := &container.Config{
		Image: "kademlia-nft-node:latest",
		Env: []string{
			"NODE_ID=" + nodeName,
			"DATA_DIR=/data",
			"SEEDER_ADDR=" + seederAddr,
		},
		ExposedPorts: nat.PortSet{port: struct{}{}},
		Labels: map[string]string{
			"com.docker.compose.project": "kademlia-nft",
			"com.docker.compose.service": nodeName, // es. "node12"
			"com.docker.compose.version": "2",
		},
	}

	hostConfig := &container.HostConfig{
		Binds: []string{fmt.Sprintf("%s:/data", hostDataPath)},
		PortBindings: nat.PortMap{
			port: []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: hostPort}},
		},
		RestartPolicy: container.RestartPolicy{Name: "unless-stopped"},
	}

	networkingConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			"kademlia-nft_kadnet": {
				Aliases: []string{nodeName}, // es. "node12"
			},
		},
	}

	// Nome stile Compose: kademlia-nft-node12-1
	name := fmt.Sprintf("kademlia-nft-%s-1", nodeName)

	resp, err := cli.ContainerCreate(ctx, config, hostConfig, networkingConfig, nil, name)
	if err != nil {
		return err
	}

	// *** AVVIA il container ***
	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		return err
	}

	fmt.Printf("✅ Nodo %s (%s) avviato sulla porta %s\n", nodeName, resp.ID[:12], hostPort)
	return nil
}

func BiggerNodes(nodi []string) (string, int) {
	var maxID = -1

	for _, n := range nodi {
		if strings.HasPrefix(n, "node") {
			idStr := strings.TrimPrefix(n, "node")
			id, err := strconv.Atoi(idStr)
			if err != nil {
				continue
			}

			if id > maxID {
				maxID = id
			}
		}
	}

	// ritorna il nuovo nodo con ID incrementato
	return "node" + strconv.Itoa(maxID+1), maxID + 1
}
