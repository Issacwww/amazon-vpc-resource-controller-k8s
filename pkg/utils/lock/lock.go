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

//go:build !deadlock

// Package lock provides Mutex/RWMutex type aliases that the controller uses in
// place of sync.Mutex/sync.RWMutex. By default they ARE sync.Mutex/sync.RWMutex
// (zero overhead, go-deadlock not compiled in). When built with the "deadlock"
// build tag (see lock_deadlock.go), they become go-deadlock's lock types, which
// detect lock-order inversions and stuck locks at runtime. This lets CI run
// `go test -tags deadlock` for deadlock detection without any cost to the
// production binary.
package lock

import "sync"

// Mutex is sync.Mutex in normal builds.
type Mutex = sync.Mutex

// RWMutex is sync.RWMutex in normal builds.
type RWMutex = sync.RWMutex
