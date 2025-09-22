package ui

import (
	"bytes"

	"os/exec"
	"strings"
)

func ListActiveComposeServices(project string) ([]string, error) {

	cmd := exec.Command("docker", "ps",
		"--filter", "label=com.docker.compose.project="+project,
		"--format", "{{.Names}}",
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	services := make([]string, 0, len(lines))
	for _, name := range lines {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		// Nome Compose tipico: <project>-<service>-<index>
		parts := strings.Split(name, "-")
		if len(parts) >= 3 {
			services = append(services, parts[len(parts)-2]) // prende <service> (es. "node1")
		}
	}
	return dedupe(services), nil
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
