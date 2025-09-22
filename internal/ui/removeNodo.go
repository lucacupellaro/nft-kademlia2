package ui

import (
	"fmt"
	"os/exec"
)

func RemoveNode(serviceName string) error {
	// -s = stop, -f = force
	cmd := exec.Command("docker", "compose", "rm", "-sf", serviceName)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("errore rimozione nodo %s: %v\nOutput: %s", serviceName, err, string(out))
	}
	fmt.Printf("✅ Nodo %s rimosso.\n", serviceName)
	return nil
}
