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

// Package lock (deadlock build) aliases Mutex/RWMutex to go-deadlock's lock
// types so that `go test -tags deadlock` detects lock-order inversions and
// stuck locks. This file is only compiled when the "deadlock" build tag is set,
// so go-deadlock never ends up in the production binary.
package lock

import "github.com/sasha-s/go-deadlock"

// Mutex is go-deadlock's Mutex when built with -tags deadlock.
type Mutex = deadlock.Mutex

// RWMutex is go-deadlock's RWMutex when built with -tags deadlock.
type RWMutex = deadlock.RWMutex
