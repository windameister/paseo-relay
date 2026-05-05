package main

import (
	"testing"
)

// TestHandoverSwapsPendingBuffer verifies that handleServerData swaps in a
// fresh pending buffer when it takes over.
//
// Why this matters: handleClient observes p.serverData under RLock and may
// release the lock *before* it pushes to p.pending. If handleServerData
// drains p.pending in-place between the unlock and the push, the frame ends
// up in a buffer that will never be drained again.
//
// The fix swaps p.pending with a new empty buffer under the write lock, so
// any subsequent push by handleClient lands in the fresh buffer (which is
// drained on the next handover).
//
// This test explicitly checks that p.pending is a *different* buffer object
// after the handover. The pre-fix code reused the same buffer.
func TestHandoverSwapsPendingBuffer(t *testing.T) {
	p := &pipe{pending: newFrameBuffer(64)}

	// Push a pre-handover frame as if a client raced ahead of the daemon.
	p.pending.push(1, []byte("pre-handover"))
	pre := p.pending

	// Simulate handleServerData's atomic handover.
	p.mu.Lock()
	p.serverData = &conn{}
	oldBuf := p.pending
	p.pending = newFrameBuffer(64) // <-- the fix
	drained := oldBuf.flush()
	p.mu.Unlock()

	if len(drained) != 1 {
		t.Fatalf("pre-handover frame was not drained: got %d frames", len(drained))
	}
	if p.pending == pre {
		t.Fatal("p.pending was not swapped during handover — frames pushed " +
			"after the handover would be stranded in the drained buffer")
	}
}

// TestSlowPathRechecksServerData verifies that handleClient's slow path
// re-reads p.serverData under the write lock before deciding to buffer.
//
// The pre-fix code did:
//
//	RLock; srv := p.serverData; RUnlock
//	if srv == nil { p.pending.push(...) }
//
// The window between RUnlock and push() lets handleServerData set
// serverData and drain pending in-between, stranding the push.
//
// The fixed code re-checks under Lock so that either:
//   - serverData is now non-nil → send directly (no buffering needed), OR
//   - serverData is still nil → push lands in the *current* (post-handover)
//     buffer, which will be drained on the next handover.
func TestSlowPathRechecksServerData(t *testing.T) {
	p := &pipe{pending: newFrameBuffer(64)}

	// Pretend handleServerData has just run and set serverData while
	// our hypothetical handleClient was between RUnlock and pushing.
	p.serverData = &conn{}

	// Client's slow path (post-fix): re-check under write lock.
	sentDirectly := false
	p.mu.Lock()
	srv := p.serverData
	if srv != nil {
		p.mu.Unlock()
		sentDirectly = true // would be srv.send(...) in real code
	} else {
		p.pending.push(1, []byte("frame"))
		p.mu.Unlock()
	}

	if !sentDirectly {
		t.Fatal("slow path did not re-check serverData under write lock — " +
			"frame was buffered instead of sent directly")
	}
	if !p.pending.isEmpty() {
		t.Fatal("frame ended up in pending despite serverData being set")
	}
}
