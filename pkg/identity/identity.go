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

package identity

import "k8s.io/apimachinery/pkg/types"

// Node identifies one Kubernetes Node generation and its EC2 instance.
type Node struct {
	Name       string
	UID        types.UID
	InstanceID string
}

// Allocation binds a Pod allocation to the Node generation that PodController
// validated.
type Allocation struct {
	Node
	PodUID types.UID
}

func Matches(expected, actual Node) bool {
	return (expected.Name == "" || expected.Name == actual.Name) &&
		(expected.UID == "" || expected.UID == actual.UID) &&
		(expected.InstanceID == "" || expected.InstanceID == actual.InstanceID)
}
