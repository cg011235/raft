package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
)

type ClusterStatus struct {
	mu            sync.RWMutex
	Idx           uint64
	NodeCount     uint
	LeaderId      uint64
	NodeIDs       []uint64
	Uptime        uint64
	CurrentTerm   uint64
	NodeEndPoints map[uint64]string
}

func (cs *ClusterStatus) UpdateLeader(leaderId uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.LeaderId = leaderId
}

func (cs *ClusterStatus) UpdateNodeCount(count uint) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.NodeCount = count
}

func (cs *ClusterStatus) AddNodeID(nodeID uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.NodeIDs = append(cs.NodeIDs, nodeID)
}

func (cs *ClusterStatus) RemoveNodeID(nodeID uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for i, id := range cs.NodeIDs {
		if id == nodeID {
			cs.NodeIDs = append(cs.NodeIDs[:i], cs.NodeIDs[i+1:]...)
			break
		}
	}
}

func (cs *ClusterStatus) UpdateUptime(uptime uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Uptime = uptime
}

func (cs *ClusterStatus) UpdateCurrentTerm(term uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.CurrentTerm = term
}

func (cs *ClusterStatus) SetNodeEndPoint(nodeID uint64, endPoint string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.NodeEndPoints == nil {
		cs.NodeEndPoints = make(map[uint64]string)
	}
	cs.NodeEndPoints[nodeID] = endPoint
}

func (cs *ClusterStatus) RemoveNodeEndPoint(nodeID uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.NodeEndPoints, nodeID)
}

type ClusterMetadata struct {
	mu               sync.RWMutex `json:"-"` // Exclude from JSON encoding/decoding
	Idx              uint64       `json:"idx"`
	NodeCount        uint         `json:"nodeCount"`
	LeaderId         uint64       `json:"leaderId"`
	NodeIDs          []uint64     `json:"nodeIds"`
	Uptime           uint64       `json:"uptime"`
	CurrentTerm      uint64       `json:"currentTerm"`
	MetadataLocation string       `json:"metadataLocation"`
}

func (metadata *ClusterMetadata) Save() error {
	// Acquire a read lock to safely read fields
	metadata.mu.RLock()
	filename := path.Join(metadata.MetadataLocation, strconv.FormatUint(metadata.Idx, 10)+".json")
	// Release the read lock before the potentially blocking I/O operation
	metadata.mu.RUnlock()

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("error creating file %s: %w", filename, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	// Re-acquire the read lock before accessing the struct's fields again
	metadata.mu.RLock()
	defer metadata.mu.RUnlock()
	if err := encoder.Encode(metadata); err != nil {
		return fmt.Errorf("error encoding cluster metadata to file %s: %w", filename, err)
	}

	return nil
}

func (metadata *ClusterMetadata) Load(clusterID uint64) error {
	filename := path.Join(metadata.MetadataLocation, strconv.FormatUint(clusterID, 10)+".json")
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file %s for loading: %w", filename, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	// Lock for writing since the struct is being modified
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	if err := decoder.Decode(metadata); err != nil {
		return fmt.Errorf("error decoding cluster metadata from file %s: %w", filename, err)
	}

	return nil
}
