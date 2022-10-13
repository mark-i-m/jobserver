//! Drives the state machine for a single task.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};
use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use chrono::offset::Utc;

use expjobserver::{cmd_replace_machine, cmd_replace_vars, cmd_to_path, human_ts};

use log::{debug, error, info, warn};

use super::{
    copier::{copy, CopierThreadQueue, CopierThreadResult, CopyJobInfo},
    JobProcessInfo, MachineStatus, Server, Task, TaskState, TaskType,
};

const UNKNOWN_HOST_ERROR_CLASS: &str = "error-unknown-host";

/// The number of consecutive job failures before the machine is considered to be faulty.
const MACHINE_FAILURES: usize = 4;

impl Server {
    /// Attempts to drive the given task's state machine forward one step.
    pub fn try_drive_sm(
        runner: &str,
        log_dir: &str,
        jid: u64,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        live_tasks: &mut BTreeSet<u64>,
        running_job_handles: &mut HashMap<u64, JobProcessInfo>,
        to_remove: &mut HashSet<u64>,
        to_clone: &mut HashSet<u64>,
        copying_flags: Arc<Mutex<HashMap<u64, CopierThreadResult>>>,
        copy_thread_queue: Arc<Mutex<CopierThreadQueue>>,
    ) -> bool {
        // If the task was canceled since the last time it was driven, set the state to `Canceled`
        // so it can get garbage collected.
        if let Some(remove) = task.canceled {
            task.update_state(TaskState::Canceled { remove });
        }

        match task.state {
            TaskState::Held => {
                info!("Task {} is held.", jid);
                false
            }

            TaskState::Waiting => Self::try_drive_sm_waiting(
                runner,
                log_dir,
                jid,
                task,
                machines,
                running_job_handles,
            ),

            TaskState::Running { index } => Self::try_drive_sm_running(
                runner,
                log_dir,
                jid,
                task,
                machines,
                running_job_handles,
                index,
            ),

            TaskState::CheckingResults => Self::try_drive_sm_checking_results(
                jid,
                task,
                machines,
                running_job_handles,
                copy_thread_queue,
                log_dir,
            ),

            TaskState::CopyingResults { ref results_path } => {
                match copying_flags.lock().unwrap().remove(&jid) {
                    Some(CopierThreadResult::Success) => {
                        debug!("Copy results for task {} completed.", jid);
                        let results_path = results_path.clone();
                        task.update_state(TaskState::Finalize {
                            results_path: Some(results_path),
                        });
                        true
                    }
                    Some(CopierThreadResult::OtherFailure) => {
                        debug!("Copy results for task {} failed.", jid);
                        task.update_state(TaskState::Error {
                            error: "Task completed, but copying results failed.".into(),
                            n: 0,
                        });
                        true
                    }
                    Some(CopierThreadResult::SshUnknownHostKey) => {
                        let machine = task.machine.clone().expect("Running task has no machine.");
                        // I think the "unwrap" case should never happen.
                        let class = task.class.clone().unwrap_or("<none>".into());

                        debug!(
                            "Copy results for task {} failed due to \
                             unknown host SSH key fingerprint. Automatically \
                             moving machine {} to {} class. \
                             Please add the machine to known_hosts and move \
                             it back to class {}.",
                            jid, machine, UNKNOWN_HOST_ERROR_CLASS, class
                        );

                        // Move the machine to the UNKNOWN_HOST_ERROR_CLASS class. The job's state
                        // transition will then move the free the machine to the appropriate class.
                        if let Some(machine) = machines.get_mut(&machine) {
                            machine.class = UNKNOWN_HOST_ERROR_CLASS.into();
                        }

                        // Change the task state.
                        task.update_state(TaskState::Error {
                            error: "Copying results failed. Please add host to SSH known_hosts \
                                and move it back to the appropriate class."
                                .into(),
                            n: 0,
                        });
                        true
                    }
                    None => false,
                }
            }

            TaskState::Finalize { ref results_path } => {
                let results_path = results_path.clone();

                Self::try_drive_sm_finalize(jid, task, machines, running_job_handles, results_path)
            }
            TaskState::Done
            | TaskState::DoneWithResults { .. }
            | TaskState::Killed
            | TaskState::Unknown { .. }
            | TaskState::ErrorDone { .. } => {
                let old = running_job_handles.remove(&jid);

                // Remove from live_tasks if it is there.
                live_tasks.remove(&jid);

                // Update occured if we actually removed something.
                old.is_some()
            }

            TaskState::Error { n, ref error } => {
                // If there is an associated machine, increase its error count.
                if let Some(machine) = task.machine.as_ref() {
                    if let Some(machine_status) = machines.get_mut(machine) {
                        machine_status.failures += 1;
                    }
                }

                // Free any associated machine.
                Self::free_machine(jid, task, machines);

                // Clean up job handles
                let _ = running_job_handles.remove(&jid).is_some();

                // If this task is marked `repeat_on_fail` (and there are retries remaining), then
                // we will need to clone the task, but we can't do so here because it creates a lot
                // of borrow checker errors (due to lots of things already being borrowed).
                if task.repeat_on_fail {
                    match task.maximum_failures {
                        Some(maximum_failures) if task.attempt >= maximum_failures => {
                            // Exceeded retry limit! Do not clone.
                        }
                        _ => {
                            // No retry limit or not exceeded.
                            to_clone.insert(jid);
                        }
                    }
                }

                // Move to the done state.
                let error = error.clone();
                task.update_state(TaskState::ErrorDone { n, error });

                true
            }

            TaskState::Canceled { remove } => Self::try_drive_sm_canceled(
                jid,
                task,
                machines,
                running_job_handles,
                to_remove,
                remove,
            ),
        }
    }

    /// The task has not started yet. Attempt to do so now.
    fn try_drive_sm_waiting(
        runner: &str,
        log_dir: &str,
        jid: u64,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        running_job_handles: &mut HashMap<u64, JobProcessInfo>,
    ) -> bool {
        let machine = if let TaskType::SetupTask = task.ty {
            Some(task.machine.as_ref().unwrap().clone())
        } else {
            machines
                .iter_mut()
                .filter(|m| m.1.running.is_none())
                .filter(|m| m.1.class.as_str() == task.class.as_ref().unwrap())
                .map(|(m, _)| m.clone())
                .next()
        };

        // If we found a machine, start the task.
        if let Some(machine) = machine {
            // Mark the machine as running the task if it exists (it may not exist if this
            // is a setup task because the machine is not setup yet).
            if let Some(machine) = machines.get_mut(&machine) {
                machine.running = Some(jid);
            }

            // Mark the task as running.
            task.update_state(TaskState::Running { index: 0 });
            task.machine = Some(machine.clone());
            task.timestamp = Utc::now();

            let slack_msg = match Self::run_cmd(jid, task, &machine, runner, log_dir) {
                Ok(job) => {
                    let msg = format!("Running job {} on machine {}", jid, machine);

                    info!("{msg}");
                    running_job_handles.insert(jid, job);

                    msg
                }
                Err(err) => {
                    let msg = format!("Unable to start job {}: {}", jid, err);

                    error!("{msg}");
                    task.update_state(TaskState::Error {
                        error: format!("Unable to start job {}: {}", jid, err),
                        n: 0, // first cmd failed
                    });
                    task.done_timestamp = Some(Utc::now());

                    msg
                }
            };
            task.send_notification(&slack_msg);

            true
        } else {
            debug!("No machine found for task {}.", jid);

            false
        }
    }

    /// The task is currently running. Check if it has terminated. If it has terminated, the
    /// we attempt to start the next command, if any, or move to copy results, if any.
    fn try_drive_sm_running(
        runner: &str,
        log_dir: &str,
        jid: u64,
        task: &mut Task,
        _machines: &mut HashMap<String, MachineStatus>,
        running_job_handles: &mut HashMap<u64, JobProcessInfo>,
        idx: usize,
    ) -> bool {
        let job_proc_info = running_job_handles
            .get_mut(&jid)
            .expect("No job process info for running command.");

        let machine = task.machine.as_ref().unwrap().clone();

        match job_proc_info.handle.try_wait() {
            // Job terminated.
            Ok(Some(status)) => {
                info!(
                    "Task {} (idx={}) terminated with status code {}.",
                    jid, idx, status
                );

                // If status code indicates sucess, then proceed. Otherwise, fail the task.
                if status.success() {
                    // If this is the last command of the task, then we are done.
                    // Otherwise, start the next command.
                    if idx == task.cmds.len() - 1 {
                        info!("Task {} is complete. Need to check for results.", jid);
                        task.update_state(TaskState::CheckingResults);
                    } else {
                        // Move to the next task and then attempt to run it.
                        task.update_state(TaskState::Running { index: idx + 1 });

                        // Actually start the command now.
                        match Self::run_cmd(jid, task, &machine, runner, log_dir) {
                            Ok(job) => {
                                info!(
                                    "Running job {} (idx={}) on machine {}",
                                    jid,
                                    idx + 1,
                                    machine
                                );

                                running_job_handles.insert(jid, job);
                            }
                            Err(err) => {
                                error!("Unable to start job {} (idx={}): {}", jid, idx + 1, err);

                                task.update_state(TaskState::Error {
                                    error: format!(
                                        "Unable to start job {} (command {}): {}",
                                        jid,
                                        idx + 1,
                                        err
                                    ),
                                    n: idx + 1,
                                });
                                task.done_timestamp = Some(Utc::now());
                            }
                        };
                    }
                } else {
                    task.update_state(TaskState::Error {
                        error: format!("Command {} failed: {}", idx, status),
                        n: idx,
                    });
                    task.done_timestamp = Some(Utc::now());
                }

                true
            }

            // Not ready yet... do nothing.
            Ok(None) => {
                debug!("Task {} is still running", jid);

                if task
                    .timeout
                    .map_or(false, |timeout| Utc::now() - task.timestamp >= timeout)
                {
                    info!("Task {} timedout. Cancelling.", jid);
                    task.timedout = Some(idx);
                    task.canceled = Some(false); // don't forget, just cancel.

                    true
                } else {
                    false
                }
            }

            // There was an error waiting for the child... not clear what to do here... for
            // now, we just do nothing and hope it works next time.
            Err(err) => {
                error!("Unable to wait for process exit for job {}: {}", jid, err);
                false
            }
        }
    }

    /// Need to copy results for a task that completed, if any.
    fn try_drive_sm_checking_results(
        jid: u64,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        _running_job_handles: &mut HashMap<u64, JobProcessInfo>,
        copy_thread_queue: Arc<Mutex<CopierThreadQueue>>,
        log_dir: &str,
    ) -> bool {
        // The job must have run successfully to get here, so we can reset the machine's failure
        // count. The machine can be `None` if it is a setup task.
        if let Some(machine_status) = machines.get_mut(task.machine.as_ref().unwrap()) {
            machine_status.failures = 0;
        }

        // Look through the stdout for the "RESULTS: " line to get the results path.
        let results_path = {
            let cmd = task.cmds.last().unwrap();
            let cmd = cmd_replace_machine(
                &cmd_replace_vars(&cmd, &task.variables),
                &task.machine.as_ref().unwrap(),
            );
            let stdout_file_name = format!("{}", cmd_to_path(jid, &cmd, log_dir));
            let file = std::fs::File::open(stdout_file_name).expect("Unable to open stdout file");
            BufReader::new(file)
                .lines()
                .filter_map(|line| line.ok())
                .find_map(|line| {
                    // Check if there was a results path printed.
                    if line.starts_with("RESULTS: ") {
                        Some(line[9..].to_string())
                    } else {
                        None
                    }
                })
        };

        // If there is such a path, then spawn a thread to copy the file to this machine
        match (&task.cp_results, &results_path) {
            (Some(cp_results), Some(results_path)) => {
                let machine = task.machine.as_ref().unwrap().clone();

                copy(
                    copy_thread_queue,
                    CopyJobInfo {
                        jid,
                        machine,
                        to: cp_results.clone(),
                        from: results_path.clone(),
                        attempt: 0,
                    },
                );

                task.update_state(TaskState::CopyingResults {
                    results_path: results_path.clone(),
                });
            }

            (Some(_), None) => {
                warn!("Task {} expected results, but none were produced.", jid);

                task.update_state(TaskState::Finalize { results_path: None });
            }

            (None, Some(_)) => {
                warn!(
                    "Discarding results for task {} even though they were produced.",
                    jid
                );

                task.update_state(TaskState::Finalize { results_path: None });
            }

            (None, None) => {
                info!("Task {} completed without results.", jid);

                task.update_state(TaskState::Finalize { results_path: None });
            }
        }

        true
    }

    fn try_drive_sm_finalize(
        jid: u64,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        _running_job_handles: &mut HashMap<u64, JobProcessInfo>,
        results_path: Option<String>,
    ) -> bool {
        let machine = task.machine.as_ref().unwrap();

        // If this is a setup task with a class, add the machine to the class. If this is a job,
        // free the machine.
        match (&task.ty, &task.class) {
            (TaskType::SetupTask, Some(class)) => {
                info!(
                    "Adding machine {} to class {} after setup task {} completed.",
                    machine, class, jid
                );

                // Check if the machine is already there, since it may be running a job.
                let old = machines.get(machine);

                let running_job = if let Some(old_class) = old {
                    error!(
                        "After setup task {}: Removing {} from old class {}. New class is {}",
                        jid, machine, old_class.class, class
                    );

                    old_class.running
                } else {
                    None
                };

                info!(
                    "Add machine {}/{} with running job: {:?}",
                    machine, class, running_job
                );

                // Add machine (or free it if it was already part of the pool)
                let already = Self::free_machine(jid, task, machines);
                if !already {
                    machines.insert(
                        machine.clone(),
                        MachineStatus {
                            class: class.clone(),
                            running: running_job,
                            failures: 0,
                        },
                    );
                }
            }

            (TaskType::SetupTask, None) | (TaskType::Job, _) => {
                // If this is a setup task, we still want to free machines that were already part
                // of the pool.
                info!("Releasing machine {:?}", machine);
                Self::free_machine(jid, task, machines);
            }
        }

        // Move to a Done state.
        if results_path.is_some() {
            task.update_state(TaskState::DoneWithResults {
                results_path: results_path.unwrap(),
            });
        } else {
            task.update_state(TaskState::Done);
        }
        task.done_timestamp = Some(Utc::now());

        true
    }

    fn try_drive_sm_canceled(
        jid: u64,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        running_job_handles: &mut HashMap<u64, JobProcessInfo>,
        to_remove: &mut HashSet<u64>,
        remove: bool,
    ) -> bool {
        info!("Canceling task {}", jid);

        // Kill any running processes
        if let Some(mut handle) = running_job_handles.remove(&jid) {
            info!("Killing task {} process (PID={})", jid, handle.handle.id());
            let _ = handle.handle.kill(); // SIGKILL
        }

        // Free any associated machine.
        Self::free_machine(jid, task, machines);

        // Add to the list to be reaped, if the job is to be removed.
        if remove {
            to_remove.insert(jid);
        } else {
            // If timed out, then Fail rather than Kill.
            if let Some(n) = task.timedout {
                task.update_state(TaskState::Error {
                    error: format!("Timed out after {}", human_ts(task.timeout.unwrap())),
                    n,
                });
            } else {
                task.update_state(TaskState::Killed);
            }
            task.canceled = None; // No need to cancel it again!
            task.done_timestamp = Some(Utc::now());
        }

        true
    }

    fn run_cmd(
        jid: u64,
        task: &Task,
        machine: &str,
        runner: &str,
        log_dir: &str,
    ) -> std::io::Result<JobProcessInfo> {
        let cmd_idx = match task.state {
            TaskState::Running { index } => index,
            _ => unreachable!(),
        };
        let cmd = &task.cmds[cmd_idx];
        let cmd = cmd_replace_machine(&cmd_replace_vars(&cmd, &task.variables), &machine);

        // The job will output stdout and stderr to these files. When the job completes, we will
        // search through the stdout file to get the name of the results file to copy, if any.

        let stdout_file_name = format!("{}", cmd_to_path(jid, &cmd, log_dir));
        let stdout_file = OpenOptions::new()
            .truncate(true)
            .write(true)
            .create(true)
            .open(&stdout_file_name)?;

        let stderr_file_name = format!("{}.err", stdout_file_name);
        let stderr_file = OpenOptions::new()
            .truncate(true)
            .write(true)
            .create(true)
            .open(&stderr_file_name)?;

        info!("Starting command: {} {}", runner, cmd);

        let mut child = std::process::Command::new(runner);
        child
            .arg("--print_results_path")
            .args(&cmd.split_whitespace().collect::<Vec<_>>())
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file));
        unsafe {
            child.pre_exec(|| {
                // Resistance to window manager crashes, etc... similar to running with `nohup`.
                let _ = libc::signal(libc::SIGHUP, libc::SIG_IGN);
                Ok(())
            })
        };

        // Start!
        let handle = child.spawn()?;

        Ok(JobProcessInfo {
            child,
            handle,
            stdout: stdout_file_name,
            stderr: stderr_file_name,
        })
    }

    /// Frees the machine if it is associated with the given task. Does nothing if this machine is
    /// not running this task. Returns true if a machine was freed.
    fn free_machine(jid: u64, task: &Task, machines: &mut HashMap<String, MachineStatus>) -> bool {
        // We need to ensure that the machine is running this task and not another, since it may
        // have been freed and allocated already.
        if let Some(machine) = task.machine.as_ref() {
            if let Some(machine_status) = machines.get_mut(machine) {
                if let Some(running_task) = machine_status.running {
                    if running_task == jid {
                        info!("Freeing machine {} used by task {}", machine, jid);
                        machine_status.running = None;

                        // If the failure count exceeds the threshold, move the machine to a
                        // different class.
                        if machine_status.failures >= MACHINE_FAILURES {
                            machine_status.class = format!("{}-broken", machine_status.class);
                        }

                        return true;
                    }
                }
            }
        }

        return false;
    }
}
