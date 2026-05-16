//go:build !windows

package cmd

import (
	"fmt"
	"os"
	"syscall"
)

// acquireSpawnLock takes an exclusive flock on the given file (creating
// it if necessary) and returns a release function that unlocks and
// closes the file. Blocks until the lock is acquired.
func acquireSpawnLock(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open spawn lock %q: %v", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("flock spawn lock %q: %v", path, err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
