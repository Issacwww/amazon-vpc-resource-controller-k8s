// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

//go:build deadlock

// These tests only compile/run under `-tags deadlock`. They verify the
// go-deadlock detector that backs pkg/utils/lock is actually wired up and
// reporting, so that a green `go test -tags deadlock ./...` run is meaningful.
package lock_test

import (
	"io"
	"sync"
	"testing"

	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/utils/lock"
	"github.com/sasha-s/go-deadlock"
)

// runAndCapture swaps go-deadlock's OnPotentialDeadlock callback so a detected
// inversion is recorded instead of terminating the process (its default), runs
// body, and restores the callback. It returns whether a potential deadlock was
// reported. Tests must run serially (no t.Parallel) since Opts is global.
func runAndCapture(t *testing.T, body func()) bool {
	t.Helper()
	var mu sync.Mutex
	detected := false

	prevCb := deadlock.Opts.OnPotentialDeadlock
	prevBuf := deadlock.Opts.LogBuf
	deadlock.Opts.OnPotentialDeadlock = func() {
		mu.Lock()
		detected = true
		mu.Unlock()
	}
	deadlock.Opts.LogBuf = io.Discard // keep the report out of test output
	t.Cleanup(func() {
		deadlock.Opts.OnPotentialDeadlock = prevCb
		deadlock.Opts.LogBuf = prevBuf
	})

	body()

	mu.Lock()
	defer mu.Unlock()
	return detected
}

// TestDetector_FlagsLockOrderInversion is the positive control: acquiring two
// locks in opposite orders (A->B then B->A) MUST be flagged. If this ever stops
// firing, the detector is not actually active and a green deadlock run is
// worthless.
func TestDetector_FlagsLockOrderInversion(t *testing.T) {
	var a, b lock.Mutex

	detected := runAndCapture(t, func() {
		a.Lock()
		b.Lock()
		b.Unlock()
		a.Unlock()

		b.Lock()
		a.Lock() // opposite order -> inversion
		a.Unlock()
		b.Unlock()
	})

	if !detected {
		t.Fatal("expected go-deadlock to flag the A->B / B->A lock-order inversion, but it did not")
	}
}

// TestDetector_AllowsConsistentLockOrder is the happy path: always acquiring the
// locks in the same order (A->B) must NOT be flagged.
func TestDetector_AllowsConsistentLockOrder(t *testing.T) {
	var a, b lock.Mutex

	detected := runAndCapture(t, func() {
		a.Lock()
		b.Lock()
		b.Unlock()
		a.Unlock()

		a.Lock() // same order again
		b.Lock()
		b.Unlock()
		a.Unlock()
	})

	if detected {
		t.Fatal("consistent A->B lock order should not be flagged as a potential deadlock")
	}
}
