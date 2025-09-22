package logica

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type byteMappingFile struct {
	List   []string `json:"list"`
	IdsHex []string `json:"ids_hex"`
}

// Salva ByteMapping.List e ByteMapping.Ids in un file JSON.
// Supporta Ids come [][]byte o [][20]byte (o in generale array/slice di byte).
func SaveByteMappingJSON(path string, bm *ByteMapping) error {
	if bm == nil {
		return fmt.Errorf("ByteMapping è nil")
	}

	idsBytes, err := bytesSliceFromAny(bm.IDs)
	if err != nil {
		return fmt.Errorf("conversione Ids: %w", err)
	}
	if len(bm.List) != len(idsBytes) {
		return fmt.Errorf("mismatch lunghezze: List=%d Ids=%d", len(bm.List), len(idsBytes))
	}

	idsHex := make([]string, len(idsBytes))
	for i, b := range idsBytes {
		idsHex[i] = hex.EncodeToString(b)
	}

	payload := byteMappingFile{
		List:   append([]string(nil), bm.List...), // copia difensiva
		IdsHex: idsHex,
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !os.IsExist(err) {
		return fmt.Errorf("creazione cartella %q: %w", filepath.Dir(path), err)
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal JSON: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("scrittura tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename su %q: %w", path, err)
	}
	return nil
}

// Ricarica dal JSON precedentemente salvato.
// Restituisce la lista e gli ID in bytes (decodificati dall'hex).
func LoadByteMappingJSON(path string) ([]string, [][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("lettura %q: %w", path, err)
	}
	var payload byteMappingFile
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, nil, fmt.Errorf("unmarshal JSON: %w", err)
	}
	if len(payload.List) != len(payload.IdsHex) {
		return nil, nil, fmt.Errorf("mismatch lunghezze nel file: list=%d ids=%d", len(payload.List), len(payload.IdsHex))
	}

	ids := make([][]byte, len(payload.IdsHex))
	for i, hx := range payload.IdsHex {
		b, err := hex.DecodeString(hx)
		if err != nil {
			return nil, nil, fmt.Errorf("decode hex id[%d]: %w", i, err)
		}
		ids[i] = b
	}
	return payload.List, ids, nil
}

// Converte in [][]byte gestendo sia [][]byte che array di byte (es. [20]byte).
func bytesSliceFromAny(ids interface{}) ([][]byte, error) {
	v := reflect.ValueOf(ids)
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("Ids non è una slice (è %s)", v.Kind())
	}
	out := make([][]byte, v.Len())
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i)
		switch e.Kind() {
		case reflect.Slice:
			// [][]byte
			if e.Type().Elem().Kind() != reflect.Uint8 {
				return nil, fmt.Errorf("Ids[%d] non è []byte", i)
			}
			b := make([]byte, e.Len())
			copy(b, e.Bytes())
			out[i] = b
		case reflect.Array:
			// es. [20]byte
			if e.Type().Elem().Kind() != reflect.Uint8 {
				return nil, fmt.Errorf("Ids[%d] non è [N]byte", i)
			}
			b := make([]byte, e.Len())
			reflect.Copy(reflect.ValueOf(b), e)
			out[i] = b
		default:
			return nil, fmt.Errorf("Ids[%d] ha tipo non supportato: %s", i, e.Kind())
		}
	}
	return out, nil
}

func HexToString(hexStr string) (string, error) {
	hexStr = strings.TrimSpace(hexStr)
	hexStr = strings.TrimPrefix(hexStr, "0x")    // gestisce prefisso "0x"
	hexStr = strings.ReplaceAll(hexStr, " ", "") // rimuove spazi
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

var reNode = regexp.MustCompile(`^node(\d+)$`)

func nextNodeNameAndPort(nodi []string) (string, string) {
	maxN := 0
	for _, s := range nodi {
		s = strings.TrimSpace(s)
		m := reNode.FindStringSubmatch(s)
		if len(m) == 2 {
			if n, err := strconv.Atoi(m[1]); err == nil && n > maxN {
				maxN = n
			}
		}
	}
	newN := maxN + 1 // es. se max è 11 → newN=12
	nodeName := fmt.Sprintf("node%d", newN)
	hostPort := strconv.Itoa(8000 + newN) // 8000+12 = 8012
	return nodeName, hostPort
}

func isPortFree(port string) bool {
	ln, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}
