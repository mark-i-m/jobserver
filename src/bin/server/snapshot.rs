//! Utilities and types for snapshotting server state.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use log::{error, info};

use super::*;

use prost::{Enumeration, Message, Oneof};

/// A snapshot of the current server state to serialize to disk.
#[derive(Clone, Message)]
struct Snapshot {
    #[prost(map = "string, message", tag = "1")]
    machines: HashMap<String, SnapshotMachineStatus>,

    #[prost(map = "string, string", tag = "2")]
    variables: HashMap<String, String>,

    #[prost(map = "uint64, message", tag = "3")]
    tasks: HashMap<u64, SnapshotTask>,

    #[prost(map = "uint64, message", tag = "4")]
    matrices: HashMap<u64, SnapshotMatrix>,

    #[prost(uint64, tag = "5")]
    next_jid: u64,
}

#[derive(Clone, Debug, PartialEq, Enumeration)]
#[repr(i32)]
enum SnapshotTaskType {
    Job = 0,
    SetupTask = 1,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotResultsPath {
    #[prost(string, optional, tag = "1")]
    results_path: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct IsRemoved {
    #[prost(bool, tag = "1")]
    remove: bool,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotTaskError {
    #[prost(string, tag = "1")]
    error: String,
    #[prost(uint64, tag = "2")]
    n: u64,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotUnknown {
    #[prost(string, optional, tag = "1")]
    machine: Option<String>,
}

/// A state machine for tasks, defining deterministically what they should do next.
#[derive(Clone, PartialEq, Oneof)]
enum SnapshotTaskState {
    /// This task has not yet started running. The value is a dummy.
    #[prost(uint64, tag = "1")]
    Waiting(u64),

    /// This task has not yet started running and is on hold. The value is a dummy.
    #[prost(uint64, tag = "2")]
    Held(u64),

    /// This task is running the `n`th command in the `cmds` vector.
    #[prost(uint64, tag = "3")]
    Running(u64),

    /// The task terminated with a successful error code, but we have not yet checked for results.
    /// The value is a dummy.
    #[prost(uint64, tag = "4")]
    CheckingResults(u64),

    /// The task completed and we are copying results.
    #[prost(message, tag = "5")]
    CopyingResults(SnapshotResultsPath),

    /// Everything is done, and we are about to move to a Done state.
    #[prost(message, tag = "6")]
    Finalize(SnapshotResultsPath),

    /// This task was canceled. If `remove` is true, then it will be garbage collected.
    #[prost(message, tag = "7")]
    Canceled(IsRemoved),

    /// This task has completed. The value is a dummy.
    #[prost(uint64, tag = "8")]
    Done(u64),

    /// This task has completed and results were produced.
    #[prost(message, tag = "9")]
    DoneWithResults(SnapshotResultsPath),

    /// This task terminated with an error. `n` represents to command index of the failed task.
    #[prost(message, tag = "10")]
    Error(SnapshotTaskError),

    /// This task was killed/canceled, but not garbage collected. The value is an error.
    #[prost(uint64, tag = "11")]
    Killed(u64),

    /// This task is in an unknown state.
    #[prost(message, tag = "12")]
    Unknown(SnapshotUnknown),
}

/// Information about a single task (a job or setup task). This is a big state machine that says
/// what the status of the task is and has all information to do the next thing when ready.
#[derive(Clone, PartialEq, Message)]
struct SnapshotTask {
    /// The tasks's ID.
    #[prost(uint64, tag = "1")]
    jid: u64,

    /// The type of the task (a job or setup task).
    #[prost(enumeration = "SnapshotTaskType", tag = "2")]
    ty: i32,

    /// (optional) The machine we are running the task on, if any.
    #[prost(string, optional, tag = "3")]
    machine: Option<String>,

    /// The class of the machines that can run this task, if any.
    #[prost(string, optional, tag = "4")]
    class: Option<String>,

    /// The location to copy results, if any.
    #[prost(string, optional, tag = "5")]
    cp_results: Option<String>,

    /// The mapping of variables at the time the job was created.
    #[prost(map = "string, string", tag = "6")]
    variables: HashMap<String, String>,

    /// The commands (without replacements).
    #[prost(string, repeated, tag = "7")]
    cmds: Vec<String>,

    /// Set to `Some` if the task is canceled; if `Some(true)` the task should be gc'ed.
    #[prost(bool, optional, tag = "8")]
    canceled: Option<bool>,

    /// The state of the task that we are currently in. This defines a state machine for the task.
    #[prost(
        oneof = "SnapshotTaskState",
        tags = "9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20"
    )]
    state: Option<SnapshotTaskState>,
}

/// A collection of jobs that run over the cartesian product of some set of variables.
#[derive(Clone, PartialEq, Message)]
struct SnapshotMatrix {
    /// This matrix's ID.
    #[prost(uint64, tag = "1")]
    id: u64,

    /// The command (without replacements).
    #[prost(string, tag = "2")]
    cmd: String,

    /// The class of the machines that can run this job.
    #[prost(string, tag = "3")]
    class: String,

    /// (optional) The location to copy results, if any.
    #[prost(string, optional, tag = "4")]
    cp_results: Option<String>,

    /// The variables and their possible values.
    #[prost(map = "string, message", tag = "5")]
    variables: HashMap<String, protocol::MatrixVarValues>,

    /// A list of jobs in this matrix.
    #[prost(uint64, repeated, tag = "6")]
    jids: Vec<u64>,
}

/// Information about a single machine.
#[derive(PartialEq, Clone, Eq, Hash, Message)]
struct SnapshotMachineStatus {
    /// The class of the machine.
    #[prost(string, tag = "1")]
    class: String,

    /// What job it is running, if any.
    #[prost(uint64, optional, tag = "2")]
    running: Option<u64>,
}

impl From<&MachineStatus> for SnapshotMachineStatus {
    fn from(stat: &MachineStatus) -> Self {
        SnapshotMachineStatus {
            class: stat.class.clone(),
            running: stat.running,
        }
    }
}

impl SnapshotTask {
    fn to_task(self, was_running: &mut BTreeMap<u64, String>) -> Task {
        Task {
            jid: self.jid,
            ty: match self.ty {
                0 => TaskType::Job,
                1 => TaskType::SetupTask,

                // Unknown types
                _ => TaskType::Job,
            },
            machine: self.machine,
            class: self.class,
            cp_results: self.cp_results,
            variables: self.variables,
            cmds: self.cmds,
            canceled: None,
            state: match self.state {
                Some(SnapshotTaskState::Waiting(_)) => TaskState::Waiting,
                Some(SnapshotTaskState::Held(_)) => TaskState::Held,
                Some(SnapshotTaskState::Canceled(IsRemoved { remove })) => {
                    TaskState::Canceled { remove }
                }
                Some(SnapshotTaskState::Done(_)) => TaskState::Done,
                Some(SnapshotTaskState::DoneWithResults(SnapshotResultsPath {
                    results_path: Some(results_path),
                })) => TaskState::DoneWithResults { results_path },
                Some(SnapshotTaskState::Error(SnapshotTaskError { error, n })) => {
                    TaskState::Error {
                        error,
                        n: n as usize,
                    }
                }
                Some(SnapshotTaskState::Killed(_)) => TaskState::Killed,
                Some(SnapshotTaskState::Unknown(SnapshotUnknown { machine })) => {
                    TaskState::Unknown { machine }
                }

                _ => TaskState::Unknown {
                    machine: was_running.remove(&self.jid),
                },
            },
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
            TaskState::Waiting => SnapshotTaskState::Waiting(0),
            TaskState::Held => SnapshotTaskState::Held(0),
            TaskState::Running(n) => SnapshotTaskState::Running(n as u64),
            TaskState::CheckingResults => SnapshotTaskState::Running(0),
            TaskState::CopyingResults { results_path } => {
                SnapshotTaskState::CopyingResults(SnapshotResultsPath {
                    results_path: Some(results_path),
                })
            }
            TaskState::Finalize { results_path } => {
                SnapshotTaskState::CopyingResults(SnapshotResultsPath { results_path })
            }
            TaskState::Canceled { remove } => SnapshotTaskState::Canceled(IsRemoved { remove }),
            TaskState::Done => SnapshotTaskState::Done(0),
            TaskState::DoneWithResults { results_path } => {
                SnapshotTaskState::DoneWithResults(SnapshotResultsPath {
                    results_path: Some(results_path),
                })
            }
            TaskState::Error { error, n } => {
                SnapshotTaskState::Error(SnapshotTaskError { error, n: n as u64 })
            }
            TaskState::Killed => SnapshotTaskState::Killed(0),
            TaskState::Unknown { machine } => {
                SnapshotTaskState::Unknown(SnapshotUnknown { machine })
            }
        };

        SnapshotTask {
            jid: task.jid,
            ty,
            machine: task.machine.clone(),
            class: task.class.clone(),
            cp_results: task.cp_results.clone(),
            variables: task.variables.clone(),
            cmds: task.cmds.clone(),
            canceled: task.canceled,
            state: Some(state),
        }
    }
}

impl From<&SnapshotMatrix> for Matrix {
    fn from(matrix: &SnapshotMatrix) -> Self {
        let variables = matrix
            .variables
            .iter()
            .map(|(k, v)| (k.clone(), v.into()))
            .collect();

        Matrix {
            id: matrix.id,
            cmd: matrix.cmd.clone(),
            class: matrix.class.clone(),
            cp_results: matrix.cp_results.clone(),
            variables,
            jids: matrix.jids.clone(),
        }
    }
}

impl From<&Matrix> for SnapshotMatrix {
    fn from(matrix: &Matrix) -> Self {
        let variables = matrix
            .variables
            .iter()
            .map(|(k, v)| (k.clone(), v.into()))
            .collect();

        SnapshotMatrix {
            id: matrix.id,
            cmd: matrix.cmd.clone(),
            class: matrix.class.clone(),
            cp_results: matrix.cp_results.clone(),
            variables,
            jids: matrix.jids.clone(),
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
            .map(|(m, s)| (m.clone(), s.into()))
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

        info!("Succeeded in loading snapshot.");

        true
    }
}
