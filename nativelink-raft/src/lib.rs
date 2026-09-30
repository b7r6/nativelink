// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A raft-replicated `AwaitedActionDb`.
//!
//! The thesis of this spike: the `AwaitedAction` lifecycle is already a formal
//! state machine. Put its transitions in a replicated log and the wall-clock /
//! version-CAS machinery in `store_awaited_action_db` becomes unrepresentable
//! rather than merely mitigated.
//!
//! - [`Command`] is the transition alphabet (add / update / heartbeat / expire).
//! - [`state::AppliedState`] mirrors `AwaitedActionDbImpl` and is the replicated
//!   state machine: the log is the only writer, so it is deterministic across
//!   replicas.
//! - [`store::Store`] implements openraft's `RaftLogStorage` + `RaftStateMachine`
//!   in memory.
//! - [`RaftAwaitedActionDb`] implements the `AwaitedActionDb` contract on top of
//!   a single-node `Raft`.

pub mod db;
pub mod state;
pub mod store;
pub mod types;

pub use db::{RaftAwaitedActionDb, RaftAwaitedActionSubscriber};
pub use types::{Command, CommandResponse, NodeId, RejectReason, TypeConfig};
