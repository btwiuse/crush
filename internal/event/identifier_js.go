//go:build js && wasm

package event

var distinctId string

const (
	hashKey    = "charm"
	fallbackId = "unknown"
)

func getDistinctId() string {
	return fallbackId
}
