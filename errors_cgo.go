//go:build cgo_raft && cgo

// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

/*
#cgo CFLAGS: -I${SRCDIR}/c/include -I${SRCDIR}/c/src
#include "raft/raft.h"
*/
import "C"

import (
	"errors"
	"fmt"
)

// ErrStepLocalMsg is returned when trying to step a local raft message through
// the public RawNode network/application boundary.
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when stepping a response from an unknown
// peer.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

var (
	errCInvalidArgument = errors.New("raft/cgo: invalid argument")
	errCNotImplemented  = errors.New("raft/cgo: C RawNode operation is not implemented")
	errCOutOfMemory     = errors.New("raft/cgo: C allocation failed")
	errCCallbackPanic   = errors.New("raft/cgo: Go callback panicked")
	errCFatal           = errors.New("raft/cgo: fatal C RawNode error")
)

func decodeCError(code C.int) error {
	switch int(code) {
	case int(C.RAFT_OK):
		return nil
	case int(C.RAFT_ERR_INVALID_ARGUMENT):
		return errCInvalidArgument
	case int(C.RAFT_ERR_STOPPED):
		return ErrStopped
	case int(C.RAFT_ERR_PROPOSAL_DROPPED):
		return ErrProposalDropped
	case int(C.RAFT_ERR_STORAGE_COMPACTED):
		return ErrCompacted
	case int(C.RAFT_ERR_STORAGE_UNAVAILABLE):
		return ErrUnavailable
	case int(C.RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE):
		return ErrSnapshotTemporarilyUnavailable
	case int(C.RAFT_ERR_STEP_LOCAL_MSG):
		return ErrStepLocalMsg
	case int(C.RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED):
		return ErrStepPeerNotFound
	case int(C.RAFT_ERR_PANIC_FROM_GO_CALLBACK):
		return errCCallbackPanic
	case int(C.RAFT_ERR_OUT_OF_MEMORY):
		return errCOutOfMemory
	case int(C.RAFT_ERR_FATAL):
		return errCFatal
	case int(C.RAFT_ERR_NOT_IMPLEMENTED):
		return errCNotImplemented
	default:
		return fmt.Errorf("raft/cgo: unknown C error code %d", int(code))
	}
}

func storageErrorCode(err error) C.int {
	switch {
	case err == nil:
		return C.RAFT_OK
	case errors.Is(err, ErrCompacted):
		return C.RAFT_ERR_STORAGE_COMPACTED
	case errors.Is(err, ErrUnavailable):
		return C.RAFT_ERR_STORAGE_UNAVAILABLE
	case errors.Is(err, ErrSnapshotTemporarilyUnavailable):
		return C.RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE
	default:
		return C.RAFT_ERR_FATAL
	}
}
