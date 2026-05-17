//go:build js && wasm

package main

import (
	"time"

	"github.com/charmbracelet/crush/internal2/interop"
	"github.com/charmbracelet/crush/internal2/js/fs"
	"github.com/charmbracelet/crush/internal2/js/process"
)

func init() {
	process.Init()
	fs.Init()
	interop.SetInitialized()

	time.Sleep(time.Second)
}
