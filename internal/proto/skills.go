package proto

import "encoding/json"

// SkillDiscoveryState represents the outcome of discovering a single skill file.
type SkillDiscoveryState int

const (
	SkillStateNormal SkillDiscoveryState = iota
	SkillStateError
)

// SkillState represents the latest discovery status of a skill file.
type SkillState struct {
	Name  string             `json:"name"`
	Path  string             `json:"path"`
	State SkillDiscoveryState `json:"state"`
	Error string             `json:"error,omitempty"`
}

// SkillEvent is published when skill discovery completes.
type SkillEvent struct {
	States []SkillState `json:"states"`
}

// MarshalJSON implements the [json.Marshaler] interface.
func (e SkillEvent) MarshalJSON() ([]byte, error) {
	type Alias SkillEvent
	return json.Marshal((Alias)(e))
}
