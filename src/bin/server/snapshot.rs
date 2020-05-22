//! Utilities and types for snapshotting server state.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;

use log::{error, info};

use serde::{Deserialize, Serialize};

use super::*;

/// A snapshot of the current server state to serialize to disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    machines: HashMap<String, MachineStatus>,
    variables: HashMap<String, String>,
    tasks: BTreeMap<u64, Task>,
    matrices: HashMap<u64, Matrix>,
    next_jid: u64,
}

impl Server {
    /// Attempt to write the server's state to a file.
    pub fn take_snapshot(&self) {
        let snapshot = Snapshot {
            machines: self.machines.lock().unwrap().clone(),
            variables: self.variables.lock().unwrap().clone(),
            tasks: self.tasks.lock().unwrap().clone(),
            matrices: self.matrices.lock().unwrap().clone(),
            next_jid: self.next_jid.load(Ordering::Relaxed),
        };

        let filename = format!("{}/{}", self.log_dir, DUMP_FILENAME);
        let snapshot_json = match serde_json::to_string(&snapshot) {
            Ok(json) => json,
            Err(err) => {
                error!("Unable to serialize a snapshot: {}", err);
                return;
            }
        };

        match std::fs::write(&filename, snapshot_json) {
            Ok(_) => {}
            Err(err) => {
                error!("Unable to write snapshot to disk: {}", err);
                return;
            }
        }

        info!("Wrote snapshot to {}", filename);
    }

    pub fn load_snapshot(&mut self) {
        let filename = format!("{}/{}", self.log_dir, DUMP_FILENAME);

        info!("Attempting to load snapshot from {}", filename);

        let snapshot_json = match std::fs::read_to_string(filename) {
            Ok(json) => json,
            Err(err) => {
                error!("Unable to read snapshot from disk: {}", err);
                return;
            }
        };

        let Snapshot {
            machines,
            variables,
            tasks,
            matrices,
            next_jid,
        } = match serde_json::from_str(&snapshot_json) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                error!("Unable to deserialize a snapshot: {}", err);
                return;
            }
        };

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
            .map(|(jid, task)| {
                (
                    jid,
                    Task {
                        canceled: None,
                        state: match task.state {
                            state @ TaskState::Waiting
                            | state @ TaskState::Held
                            | state @ TaskState::Canceled { .. }
                            | state @ TaskState::Done
                            | state @ TaskState::DoneWithResults { .. }
                            | state @ TaskState::Error { .. }
                            | state @ TaskState::Killed
                            | state @ TaskState::Unknown { .. } => state,
                            _ => TaskState::Unknown {
                                machine: was_running.remove(&jid),
                            },
                        },
                        ..task
                    },
                )
            })
            .collect();
        *self.matrices.lock().unwrap() = matrices;
        self.next_jid.store(next_jid, Ordering::Relaxed);

        info!("Succeeded in loading snapshot.");
    }
}
