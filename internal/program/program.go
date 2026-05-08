// Package program defines the Program interface used by app.App to
// communicate with the Bubble Tea TUI, avoiding an import cycle between
// the app and workspace packages.
package program

import tea "charm.land/bubbletea/v2"

// Program is a minimal interface covering what Subscribe needs from
// a Bubble Tea program. *tea.Program satisfies it; so do test doubles
// and other TUI implementations (e.g. boba).
type Program interface {
	Send(tea.Msg)
	Quit()
}
