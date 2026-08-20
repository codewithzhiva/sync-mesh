package syncengine

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Mutation struct {
	ID        string    `json:"id"`
	ClientID  string    `json:"client_id"`
	Key       string    `json:"key"`
	Kind      string    `json:"kind"`
	Value     string    `json:"value,omitempty"`
	Timestamp int64     `json:"timestamp"`
	Sequence  uint64    `json:"sequence"`
	AppliedAt time.Time `json:"applied_at"`
}
type lwwValue struct {
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
	ClientID  string `json:"client_id"`
	Deleted   bool   `json:"deleted"`
}
type Snapshot struct {
	Cursor     uint64              `json:"cursor"`
	Registers  map[string]string   `json:"registers"`
	Sets       map[string][]string `json:"sets"`
	Tombstones []string            `json:"tombstones"`
}
type Engine struct {
	mu        sync.RWMutex
	path      string
	seen      map[string]bool
	log       []Mutation
	registers map[string]lwwValue
	sets      map[string]map[string]bool
}

func Open(path string) (*Engine, error) {
	engine := &Engine{path: path, seen: map[string]bool{}, registers: map[string]lwwValue{}, sets: map[string]map[string]bool{}}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return engine, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	for scanner.Scan() {
		var mutation Mutation
		if err := json.Unmarshal(scanner.Bytes(), &mutation); err != nil {
			return nil, err
		}
		engine.apply(mutation)
	}
	return engine, scanner.Err()
}

func (e *Engine) Apply(mutation Mutation) (Mutation, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.seen[mutation.ID] {
		for _, existing := range e.log {
			if existing.ID == mutation.ID {
				return existing, false, nil
			}
		}
	}
	if mutation.ID == "" || mutation.ClientID == "" || mutation.Key == "" {
		return Mutation{}, false, errors.New("id, client_id and key are required")
	}
	switch mutation.Kind {
	case "lww_set", "gset_add", "delete":
	default:
		return Mutation{}, false, errors.New("kind must be lww_set, gset_add or delete")
	}
	mutation.Sequence = uint64(len(e.log) + 1)
	mutation.AppliedAt = time.Now().UTC()
	if err := e.append(mutation); err != nil {
		return Mutation{}, false, err
	}
	e.apply(mutation)
	return mutation, true, nil
}
func (e *Engine) apply(m Mutation) {
	e.seen[m.ID] = true
	e.log = append(e.log, m)
	switch m.Kind {
	case "lww_set", "delete":
		current, exists := e.registers[m.Key]
		if !exists || m.Timestamp > current.Timestamp || (m.Timestamp == current.Timestamp && m.ClientID > current.ClientID) {
			e.registers[m.Key] = lwwValue{Value: m.Value, Timestamp: m.Timestamp, ClientID: m.ClientID, Deleted: m.Kind == "delete"}
		}
	case "gset_add":
		if e.sets[m.Key] == nil {
			e.sets[m.Key] = map[string]bool{}
		}
		e.sets[m.Key][m.Value] = true
	}
}
func (e *Engine) append(m Mutation) error {
	file, err := os.OpenFile(e.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	data, _ := json.Marshal(m)
	if _, err = file.Write(append(data, '\n')); err != nil {
		return err
	}
	return file.Sync()
}
func (e *Engine) ChangesSince(cursor uint64, limit int) ([]Mutation, uint64) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if cursor > uint64(len(e.log)) {
		cursor = 0
	}
	end := len(e.log)
	if limit > 0 && int(cursor)+limit < end {
		end = int(cursor) + limit
	}
	out := append([]Mutation(nil), e.log[cursor:end]...)
	return out, uint64(end)
}
func (e *Engine) Snapshot() Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	snapshot := Snapshot{Cursor: uint64(len(e.log)), Registers: map[string]string{}, Sets: map[string][]string{}}
	for key, value := range e.registers {
		if value.Deleted {
			snapshot.Tombstones = append(snapshot.Tombstones, key)
		} else {
			snapshot.Registers[key] = value.Value
		}
	}
	for key, values := range e.sets {
		for value := range values {
			snapshot.Sets[key] = append(snapshot.Sets[key], value)
		}
		sort.Strings(snapshot.Sets[key])
	}
	sort.Strings(snapshot.Tombstones)
	return snapshot
}
