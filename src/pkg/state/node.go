package state

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path"
	"strconv"
	"sync"
)

const (
	Follower                = 0
	Candidate               = 1
	Leader                  = 2
	ClusterMetadataLocation = "/raft/cluster/"
	NodeMetaDataLocation    = "/raft/cluster/nodes/"
	Unknown                 = 0
	Failed                  = 1
	Active                  = 2
)

type NodeMetadata struct {
	mu          sync.RWMutex
	Idx         uint64 `json:"idx"`
	IP          net.IP `json:"ip"`
	Port        int    `json:"port"`
	UpTime      uint64 `json:"uptime"`
	Role        uint   `json:"role"`
	LogLocation string `json:"logLocation"`
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    uint64 `json:"votedFor"`
}

func (metadata *NodeMetadata) Save() error {
	// Acquire a read lock to safely read fields
	metadata.mu.RLock()
	filename := path.Join(NodeMetaDataLocation, strconv.FormatUint(metadata.Idx, 10)+".json")
	// Release the read lock before potentially blocking I/O operation
	metadata.mu.RUnlock()

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("error opening file %s: %w", filename, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	// Re-acquire the read lock before accessing the struct's fields again
	metadata.mu.RLock()
	defer metadata.mu.RUnlock()
	if err := encoder.Encode(metadata); err != nil {
		return fmt.Errorf("error encoding metadata to file %s: %w", filename, err)
	}

	return nil
}

func (metadata *NodeMetadata) Load(nodeID uint64) error {
	filename := path.Join(NodeMetaDataLocation, strconv.FormatUint(nodeID, 10)+".json")
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file %s for loading: %w", filename, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	// Lock for writing since the struct is being modified
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	if err := decoder.Decode(&metadata); err != nil {
		return fmt.Errorf("error decoding metadata from file %s: %w", filename, err)
	}

	if nodeID != metadata.Idx {
		return fmt.Errorf("Node ID passed (%d) and node ID in config file (%d) do not match", nodeID, metadata.Idx)
	}

	return nil
}

type LogEntry struct {
	Term    uint64
	Command interface{}
}

type NodeStatus struct {
	mu               sync.RWMutex
	Idx              uint64
	CommitIndex      uint64
	LastApplied      uint64
	Role             uint
	State            uint
	LastHeartBeat    uint64
	HeartBeatTimeOut uint
	CurrentTerm      uint64
	VotedFor         uint64
	LastElection     uint64
	ElectionTimeOut  uint64
	Entries          []LogEntry
}

func (ns *NodeStatus) UpdateCommitIndex(index uint64) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.CommitIndex = index
}

func (ns *NodeStatus) UpdateLastApplied(index uint64) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.LastApplied = index
}

func (ns *NodeStatus) UpdateRole(role uint) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Role = role
}

func (ns *NodeStatus) UpdateState(state uint) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.State = state
}

func (ns *NodeStatus) UpdateLastHeartBeat(heartbeat uint64) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.LastHeartBeat = heartbeat
}

func (ns *NodeStatus) UpdateHeartBeatTimeOut(timeout uint) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.HeartBeatTimeOut = timeout
}

func (ns *NodeStatus) UpdateCurrentTerm(term uint64) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.CurrentTerm = term
}

func (ns *NodeStatus) UpdateVotedFor(candidateID uint64) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.VotedFor = candidateID
}

func (ns *NodeStatus) UpdateLastElection(time uint64) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.LastElection = time
}

func (ns *NodeStatus) UpdateElectionTimeOut(timeout uint64) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.ElectionTimeOut = timeout
}

func (ns *NodeStatus) AppendEntry(entry LogEntry) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Entries = append(ns.Entries, entry)
}

func (ns *NodeStatus) GetStatus() (uint64, uint64, uint64, uint, uint, uint64, uint, uint64, uint64, uint64, uint64, []LogEntry) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.Idx, ns.CommitIndex, ns.LastApplied, ns.Role, ns.State, ns.LastHeartBeat, ns.HeartBeatTimeOut, ns.CurrentTerm, ns.VotedFor, ns.LastElection, ns.ElectionTimeOut, ns.Entries
}
