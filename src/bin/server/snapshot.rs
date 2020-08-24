//! Utilities and types for snapshotting server state.
//!
//! This file is almost entirely protobuf boilerplate...

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use log::{error, info};

use super::*;
use serialize::Snapshot;

mod serialize {
    use super::super::*;

    use expjobserver::{deserialize_ts, serialize_ts};

    use log::trace;

    include!(concat!(env!("OUT_DIR"), "/state_snapshot.rs"));

    impl SnapshotTask {
        pub(super) fn to_task(self, was_running: &mut BTreeMap<u64, String>) -> Task {
            use snapshot_task_state::State;

            Task {
                jid: self.jid,
                matrix: self.matrix,
                ty: match self.ty() {
                    SnapshotTaskType::Job => TaskType::Job,
                    SnapshotTaskType::SetupTask => TaskType::SetupTask,
                },
                machine: self.machine.clone(),
                class: self.class,
                cp_results: self.cp_results,
                variables: self.variables,
                cmds: self.cmds,
                canceled: None,
                state: match self.state.state {
                    Some(State::Waiting(_)) => TaskState::Waiting,
                    Some(State::Held(_)) => TaskState::Held,
                    Some(State::Canceled(Canceled { remove })) => TaskState::Canceled { remove },
                    Some(State::Finalize(Finalize {
                        results_path: Some(results_path),
                    })) => TaskState::DoneWithResults { results_path },
                    Some(State::Finalize(Finalize { results_path: None })) => TaskState::Done,
                    Some(State::Done(_)) => TaskState::Done,
                    Some(State::Donewr(DoneWithResults { results_path })) => {
                        TaskState::DoneWithResults { results_path }
                    }
                    Some(State::Error(Error { error, n })) => TaskState::ErrorDone {
                        error,
                        n: n as usize,
                    },
                    Some(State::Killed(_)) => TaskState::Killed,

                    // The following states deserialize into Unknown states because we cannot fully
                    // recover them...
                    Some(State::Running(_))
                    | Some(State::CheckingResults(_))
                    | Some(State::CopyingResults(_)) => TaskState::Unknown {
                        machine: self.machine.clone(),
                    },

                    Some(State::Unknown(Unknown { machine })) => TaskState::Unknown { machine },

                    None => {
                        error!("Error deserializing job {}. Missing state.", self.jid);
                        TaskState::Unknown {
                            machine: was_running.remove(&self.jid),
                        }
                    }
                },
                repeat_on_fail: self.repeat_on_fail,
                timestamp: deserialize_ts(self.timestamp),
                done_timestamp: self.done_timestamp.map(deserialize_ts),
                timeout: self
                    .timeout
                    .map(|mins| chrono::Duration::minutes(mins as i64)),
                timedout: self.timedout.map(|t| t as usize),
            }
        }
    }

    impl From<&Task> for SnapshotTask {
        fn from(task: &Task) -> Self {
            let ty = match task.ty {
                TaskType::Job => SnapshotTaskType::Job,
                TaskType::SetupTask => SnapshotTaskType::SetupTask,
            }
            .into();

            let state = match task.state.clone() {
                TaskState::Waiting => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::Waiting(Waiting {})),
                },
                TaskState::Held => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::Held(Held {})),
                },
                TaskState::Running(n) => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::Running(Running { n: n as u64 })),
                },
                TaskState::CheckingResults => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::CheckingResults(
                        CheckingResults {},
                    )),
                },
                TaskState::CopyingResults { results_path } => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::CopyingResults(CopyingResults {
                        results_path,
                    })),
                },
                TaskState::Finalize { results_path } => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::Finalize(Finalize {
                        results_path,
                    })),
                },
                TaskState::Canceled { remove } => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::Canceled(Canceled { remove })),
                },
                TaskState::Done => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::Done(Done {})),
                },
                TaskState::DoneWithResults { results_path } => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::Donewr(DoneWithResults {
                        results_path,
                    })),
                },
                // We serialize both into the final error state so that we don't get tasks starting up
                // on server restarts.
                TaskState::Error { error, n } | TaskState::ErrorDone { error, n } => {
                    SnapshotTaskState {
                        state: Some(snapshot_task_state::State::Error(Error {
                            error,
                            n: n as u64,
                        })),
                    }
                }
                TaskState::Killed => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::Killed(Killed {})),
                },
                TaskState::Unknown { machine } => SnapshotTaskState {
                    state: Some(snapshot_task_state::State::Unknown(Unknown { machine })),
                },
            };

            let st = SnapshotTask {
                jid: task.jid,
                matrix: task.matrix,
                ty,
                machine: task.machine.clone(),
                class: task.class.clone(),
                cp_results: task.cp_results.clone(),
                variables: task.variables.clone(),
                cmds: task.cmds.clone(),
                canceled: task.canceled,
                state,
                repeat_on_fail: task.repeat_on_fail,
                timestamp: serialize_ts(task.timestamp),
                done_timestamp: task.done_timestamp.map(serialize_ts),
                timeout: task.timeout.map(|timeout| timeout.num_minutes() as u64),
                timedout: task.timedout.map(|t| t as u64),
            };

            trace!("Serialize: {:?}", st);

            st
        }
    }

    impl From<&SnapshotMatrix> for Matrix {
        fn from(matrix: &SnapshotMatrix) -> Self {
            let variables = matrix
                .variables
                .iter()
                .map(|(k, v)| (k.clone(), v.values.clone()))
                .collect();

            Matrix {
                id: matrix.id,
                cmd: matrix.cmd.clone(),
                class: matrix.class.clone(),
                cp_results: matrix.cp_results.clone(),
                variables,
                jids: matrix.jids.iter().map(|j| *j).collect(),
            }
        }
    }

    impl From<&Matrix> for SnapshotMatrix {
        fn from(matrix: &Matrix) -> Self {
            let variables = matrix
                .variables
                .iter()
                .map(|(k, v)| (k.clone(), MatrixVarValues { values: v.clone() }))
                .collect();

            SnapshotMatrix {
                id: matrix.id,
                cmd: matrix.cmd.clone(),
                class: matrix.class.clone(),
                cp_results: matrix.cp_results.clone(),
                variables,
                jids: matrix.jids.iter().map(|j| *j).collect(),
            }
        }
    }

    impl From<&Server> for Snapshot {
        fn from(server: &Server) -> Self {
            let machines = server
                .machines
                .lock()
                .unwrap()
                .iter()
                .map(|(m, s)| {
                    (
                        m.clone(),
                        SnapshotMachineStatus {
                            class: s.class.clone(),
                            running: s.running.clone(),
                        },
                    )
                })
                .collect();

            let tasks = server
                .tasks
                .lock()
                .unwrap()
                .iter()
                .map(|(t, s)| (*t, s.into()))
                .collect();

            let matrices = server
                .matrices
                .lock()
                .unwrap()
                .iter()
                .map(|(m, s)| (*m, s.into()))
                .collect();

            Snapshot {
                machines,
                variables: server.variables.lock().unwrap().clone(),
                tasks,
                matrices,
                next_jid: server.next_jid.load(Ordering::Relaxed),
            }
        }
    }
}

impl Server {
    /// Attempt to write the server's state to a file.
    pub fn take_snapshot(&self) {
        let snapshot = Snapshot::from(self);

        let filename = format!("{}/{}", self.log_dir, DUMP_FILENAME);

        let mut bytes = vec![];
        match snapshot.encode(&mut bytes) {
            Ok(_) => {}
            Err(err) => {
                error!("Unable to serialize a snapshot: {}", err);
                return;
            }
        };

        match std::fs::write(&filename, &bytes) {
            Ok(_) => {}
            Err(err) => {
                error!("Unable to write snapshot to disk: {}", err);
                return;
            }
        }

        info!("Wrote snapshot to {}", filename);
    }

    /// Returns true if success.
    pub fn load_snapshot(&mut self) -> bool {
        let filename = format!("{}/{}", self.log_dir, DUMP_FILENAME);

        info!("Attempting to load snapshot from {}", filename);

        let bytes = match std::fs::read(filename) {
            Ok(bytes) => bytes,
            Err(err) => {
                error!("Unable to read snapshot from disk: {}", err);
                return false;
            }
        };

        let snapshot = match Snapshot::decode(bytes.as_slice()) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                error!("Unable to deserialize a snapshot: {}", err);
                return false;
            }
        };

        let Snapshot {
            machines,
            variables,
            tasks,
            matrices,
            next_jid,
        } = snapshot;

        // We free everything since we have no way to reconnect to sessions. But we do keep track
        // of what was running so that we can report statuses well.
        let mut was_running = BTreeMap::new();

        *self.machines.lock().unwrap() = machines
            .into_iter()
            .map(|(machine, status)| {
                if let Some(running) = status.running {
                    was_running.insert(running, machine.clone());
                }

                (
                    machine,
                    MachineStatus {
                        class: status.class,
                        running: None,
                    },
                )
            })
            .collect();
        *self.variables.lock().unwrap() = variables;
        *self.tasks.lock().unwrap() = tasks
            .into_iter()
            .map(|(jid, task)| (jid, task.to_task(&mut was_running)))
            .collect();
        *self.matrices.lock().unwrap() = matrices.iter().map(|(m, s)| (*m, s.into())).collect();
        self.next_jid.store(next_jid, Ordering::Relaxed);

        // Older versions of the server would leave forgotten jids in matrices. If there are any,
        // clean them up now...
        {
            let locked_tasks = self.tasks.lock().unwrap();
            let mut locked_matrices = self.matrices.lock().unwrap();

            for matrix in locked_matrices.values_mut() {
                matrix.jids = matrix
                    .jids
                    .iter()
                    .filter(|j| locked_tasks.contains_key(j))
                    .map(|j| *j)
                    .collect();
            }
        }

        info!("Succeeded in loading snapshot.");

        true
    }
}
