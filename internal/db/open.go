package db

// Open creates a new filesystem-backed Querier for the given data directory.
// The directory and required subdirectories are created if they do not exist.
func Open(dataDir string) (Querier, error) {
	return NewFSQuerier(dataDir)
}
