//! A thread that copies results back to the host machine and notifies the server of completion.

use log::{error, info, warn};

use std::collections::{HashMap, HashSet, LinkedList};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Timeout on the copy task in minutes.
const RSYNC_TIMEOUT: u64 = 2 * 60;
/// The number of times to retry before giving up.
const RETRIES: usize = 3;

/// (jid, machine, from_path, to_path, attempt)
pub type CopyJobInfo = (u64, String, String, String, usize);
pub type CopierThreadQueue = LinkedList<CopyJobInfo>;

/// Internal state of the copier thread. This basically just keeps track of in-flight copies.
#[derive(Debug)]
struct CopierThreadState {
    /// Copy tasks that have not been started yet.
    incoming: Arc<Mutex<CopierThreadQueue>>,
    /// Copy tasks that are currently running.
    ongoing: HashMap<u64, ResultsInfo>,
    /// Used to notify the worker thread of completion.
    copying_flags: Arc<Mutex<HashSet<u64>>>,
}

/// Info about a single copy job.
#[derive(Debug)]
struct ResultsInfo {
    /// The job we are copying results for.
    jid: u64,
    /// The machine to copy results from.
    machine: String,
    /// The path on `machine` to copy from.
    from: String,
    /// The path on the host to copy to.
    to: String,
    /// The process currently doing the copy.
    process: std::process::Child,
    /// The time when the process was started.
    start_time: Instant,
    /// The current attempt number.
    attempt: usize,
}

impl std::hash::Hash for ResultsInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.jid.hash(state)
    }
}

impl std::cmp::PartialEq for ResultsInfo {
    fn eq(&self, other: &Self) -> bool {
        self.jid == other.jid
    }
}
impl std::cmp::Eq for ResultsInfo {}

/// Start the copier thread and return a handle.
pub fn init(
    copying_flags: Arc<Mutex<HashSet<u64>>>,
) -> (std::thread::JoinHandle<()>, Arc<Mutex<CopierThreadQueue>>) {
    let incoming = Arc::new(Mutex::new(LinkedList::new()));
    let state = CopierThreadState {
        incoming: incoming.clone(),
        ongoing: HashMap::default(),
        copying_flags,
    };

    (std::thread::spawn(|| copier_thread(state)), incoming)
}

/// Inform the copier thread to copy the given results from the given machine to the given
/// location. This will kill any pre-existing copy task for the same jid.
pub fn copy(queue: Arc<Mutex<CopierThreadQueue>>, task: CopyJobInfo) {
    queue.lock().unwrap().push_back(task);
}

/// The main routine of the copier thread.
fn copier_thread(mut state: CopierThreadState) {
    // We loop around checking on various tasks.
    loop {
        // Check for new tasks to start.
        while let Some(new) = state.incoming.lock().unwrap().pop_front() {
            let jid = new.0;

            // If there is an existing task, kill it.
            if let Some(mut old) = state.ongoing.remove(&jid) {
                let _ = old.process.kill();
            }

            match start_copy(new.clone()) {
                // If success, good.
                Ok(results_info) => {
                    state.ongoing.insert(jid, results_info);
                }

                // Otherwise, re-enqueue and try again later.
                Err(err) => {
                    error!(
                        "Unable to start copying process for job {}. Re-enqueuing. {}",
                        jid, err,
                    );
                    state.incoming.lock().unwrap().push_back(new);
                }
            }
        }

        // Check on ongoing copies. If they are taking too long, restart them. If they are
        // complete, notify the worker thread.
        let mut to_remove = vec![];
        for (jid, results_info) in state.ongoing.iter_mut() {
            match results_info.process.try_wait() {
                Ok(Some(exit)) if exit.success() => {
                    info!("Finished copying results for job {}.", jid,);
                    state.copying_flags.lock().unwrap().insert(*jid);
                    to_remove.push(*jid);
                }
                Ok(Some(exit)) => {
                    warn!(
                        "Copying results for job {} failed with exit code {}.",
                        jid, exit
                    );

                    // Maybe retry.
                    if results_info.attempt < RETRIES {
                        state.incoming.lock().unwrap().push_back((
                            results_info.jid,
                            results_info.machine.clone(),
                            results_info.from.clone(),
                            results_info.to.clone(),
                            results_info.attempt + 1,
                        ));
                    } else {
                        error!(
                            "Copying results for job {} failed after {} attempts.",
                            jid, RETRIES
                        );
                        state.copying_flags.lock().unwrap().insert(*jid);
                        to_remove.push(*jid);
                    }
                }
                Err(err) => {
                    error!("Error copy results for job {}: {}", jid, err);

                    // Maybe retry.
                    if results_info.attempt < RETRIES {
                        state.incoming.lock().unwrap().push_back((
                            results_info.jid,
                            results_info.machine.clone(),
                            results_info.from.clone(),
                            results_info.to.clone(),
                            results_info.attempt + 1,
                        ));
                    } else {
                        error!(
                            "Copying results for job {} failed after {} attempts.",
                            jid, RETRIES
                        );
                        state.copying_flags.lock().unwrap().insert(*jid);
                        to_remove.push(*jid);
                    }
                }
                Ok(None) => {
                    let timed_out = (Instant::now() - results_info.start_time)
                        >= Duration::from_secs(RSYNC_TIMEOUT * 60);

                    // If timed out maybe retry
                    if timed_out {
                        warn!("Copying results for job {} timed out.", jid,);
                        if results_info.attempt < RETRIES {
                            state.incoming.lock().unwrap().push_back((
                                results_info.jid,
                                results_info.machine.clone(),
                                results_info.from.clone(),
                                results_info.to.clone(),
                                results_info.attempt + 1,
                            ));
                        } else {
                            error!(
                                "Copying results for job {} failed after {} attempts.",
                                jid, RETRIES
                            );
                            state.copying_flags.lock().unwrap().insert(*jid);
                            to_remove.push(*jid);
                        }
                    }
                }
            }
        }

        for jid in to_remove.drain(..) {
            state.ongoing.remove(&jid);
        }
    }
}

/// Start copying some results.
fn start_copy(
    (jid, machine, results_path, cp_results, attempt): CopyJobInfo,
) -> Result<ResultsInfo, std::io::Error> {
    // Copy via rsync
    info!("Copying results (job {}) to {}", jid, cp_results);

    // HACK: assume all machine names are in the form HOSTNAME:PORT.
    let machine_ip = machine.split(":").next().unwrap();

    // Sometimes the command will hang spuriously. So we give it a timeout and
    // restart if needed.
    let process = std::process::Command::new("rsync")
        .arg("-z")
        .arg(&format!("{}:{}", machine_ip, results_path))
        .arg(&cp_results)
        .spawn()?;

    Ok(ResultsInfo {
        jid,
        machine,
        from: results_path,
        to: cp_results,
        process,
        start_time: std::time::Instant::now(),
        attempt,
    })
}
