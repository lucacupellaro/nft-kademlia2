package logica

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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

func RemoveNode1(nodi *[]string) {

	out := (*nodi)[:0]
	for _, s := range *nodi {
		if s != "node1" {
			out = append(out, s)
		}
	}
	*nodi = out
}

func RequireIntEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return def
}
