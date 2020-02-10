//! Server implmentation

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use clap::clap_app;

use jobserver::{
    cmd_replace_machine, cmd_replace_vars, cmd_to_path, JobServerReq, JobServerResp, Status,
    SERVER_ADDR,
};

use log::{debug, error, info, warn};

/// The server's state.
#[derive(Debug)]
struct Server {
    // Lock ordering:
    // - machines
    // - tasks
    /// Maps available machines to their classes.
    machines: Arc<Mutex<HashMap<String, MachineStatus>>>,

    /// Any variables set by the client. Used for replacement in command strings.
    variables: Arc<Mutex<HashMap<String, String>>>,

    /// Information about queued tasks, by job ID.
    tasks: Arc<Mutex<HashMap<usize, Task>>>,

    /// Information about matrices, by ID.
    matrices: Arc<Mutex<HashMap<usize, Matrix>>>,

    /// The next job ID to be assigned.
    next_jid: AtomicUsize,

    /// The path to the runner. Never changes.
    runner: String,
}

#[derive(Clone, Debug)]
enum TaskType {
    Job,
    SetupTask,
}

/// A state machine for tasks, defining deterministically what they should do next.
#[derive(Clone, Debug)]
enum TaskState {
    /// This task has not yet started running.
    Waiting,

    /// This task is running the `n`th command in the `cmds` vector.
    Running(usize),

    /// The task terminated with a successful error code, but we have not yet checked for results.
    CheckingResults,

    /// The task completed and we are copying results.
    CopyingResults { results_path: String },

    /// Everything is done, and we are about to move to a Done state.
    Finalize { results_path: Option<String> },

    /// This task has completed.
    Done,

    /// This task has completed and results were produced.
    DoneWithResults {
        /// The filename (prefix) of results from the task.
        results_path: String,
    },

    /// This task terminated with an error.
    Error { error: String },

    /// This task was canceled and has yet to be garbage collected.
    Canceled,
}

/// Information about a single task (a job or setup task). This is a big state machine that says
/// what the status of the task is and has all information to do the next thing when ready.
#[derive(Clone, Debug)]
struct Task {
    /// The tasks's ID.
    jid: usize,

    /// The type of the task (a job or setup task).
    ty: TaskType,

    /// The machine we are running the task on, if any.
    machine: Option<String>,

    /// The class of the machines that can run this task, if any.
    class: Option<String>,

    /// The location to copy results, if any.
    cp_results: Option<String>,

    /// The mapping of variables at the time the job was created.
    variables: HashMap<String, String>,

    /// The commands (without replacements).
    cmds: Vec<String>,

    /// Set to `true` if the task is canceled; `false` otherwise.
    canceled: bool,

    /// The state of the task that we are currently in. This defines a state machine for the task.
    state: TaskState,
}

/// A collection of jobs that run over the cartesian product of some set of variables.
#[derive(Clone, Debug)]
struct Matrix {
    /// This matrix's ID.
    id: usize,

    /// The command (without replacements).
    cmd: String,

    /// The class of the machines that can run this job.
    class: String,

    /// The location to copy results, if any.
    cp_results: Option<String>,

    /// The variables and their possible values.
    variables: HashMap<String, Vec<String>>,

    /// A list of jobs in this matrix.
    jids: Vec<usize>,
}

/// Information about a single machine.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MachineStatus {
    /// The class of the machine.
    class: String,

    /// What job it is running, if any.
    running: Option<usize>,
}

/// Information about a job run by the server.
#[derive(Debug)]
struct JobProcessInfo {
    child: Command,

    /// The child process running the actual job.
    handle: Child,

    /// The filename of the stdout file.
    stdout: String,

    /// The filename of the stderr file.
    stderr: String,
}

impl Task {
    pub fn status(&self) -> Status {
        match &self.state {
            TaskState::Waiting => Status::Waiting,
            TaskState::Running(..)
            | TaskState::CheckingResults { .. }
            | TaskState::CopyingResults { .. }
            | TaskState::Finalize { .. } => Status::Running {
                machine: self.machine.as_ref().unwrap().clone(),
            },
            TaskState::Done => Status::Done {
                machine: self.machine.as_ref().unwrap().clone(),
                output: None,
            },
            TaskState::DoneWithResults { results_path } => Status::Done {
                machine: self.machine.as_ref().unwrap().clone(),
                output: Some(results_path.clone()),
            },
            TaskState::Error { error } => Status::Failed {
                machine: Some(self.machine.as_ref().unwrap().clone()),
                error: error.clone(),
            },
            TaskState::Canceled => Status::Canceled,
        }
    }
}

impl Server {
    /// Creates a new server. Not listening yet.
    pub fn new(runner: String) -> Self {
        Self {
            machines: Arc::new(Mutex::new(HashMap::new())),
            variables: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            matrices: Arc::new(Mutex::new(HashMap::new())),
            next_jid: AtomicUsize::new(0),
            runner,
        }
    }

    pub fn listen(&self, listen_addr: &str) {
        let listener = match TcpListener::bind(listen_addr) {
            Ok(listener) => listener,
            Err(e) => {
                error!("Unable to listen at `{}`: {}", listen_addr, e);
                info!("Exiting");
                std::process::exit(1);
            }
        };

        // accept incoming streams and process them.
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => match self.handle_client(stream) {
                    Ok(()) => {}
                    Err(e) => error!("Error while handling client: {}", e),
                },
                Err(e) => error!("Error while handling client: {}", e),
            }
        }
    }
}

impl Server {
    fn handle_client(&self, mut client: TcpStream) -> std::io::Result<()> {
        let peer_addr = client.peer_addr()?;
        info!("Handling request from {}", peer_addr);

        let mut request = String::new();
        client.read_to_string(&mut request)?;

        let request: JobServerReq = serde_json::from_str(&request)?;

        info!("(request) {}: {:?}", peer_addr, request);

        client.shutdown(Shutdown::Read)?;

        let response = self.handle_request(request)?;

        info!("(response) {}: {:?}", peer_addr, response);

        let response = serde_json::to_string(&response)?;

        client.write_all(response.as_bytes())?;

        Ok(())
    }

    fn handle_request(&self, request: JobServerReq) -> std::io::Result<JobServerResp> {
        use JobServerReq::*;

        let response = match request {
            Ping => JobServerResp::Ok,

            MakeAvailable { addr, class } => {
                let mut locked = self.machines.lock().unwrap();

                // Check if the machine is already there, since it may be running a job.
                let old = locked.get(&addr);

                let running_job = if let Some(old_class) = old {
                    warn!(
                        "Removing {} from old class {}. New class is {}",
                        addr, old_class.class, class
                    );

                    old_class.running
                } else {
                    None
                };

                info!(
                    "Add machine {}/{} with running job: {:?}",
                    addr, class, running_job
                );

                // Add machine
                locked.insert(
                    addr.clone(),
                    MachineStatus {
                        class,
                        running: running_job,
                    },
                );

                // Respond
                JobServerResp::Ok
            }

            RemoveAvailable { addr } => {
                if let Some(old_class) = self.machines.lock().unwrap().remove(&addr) {
                    info!("Removed machine {}/{}", addr, old_class.class);

                    // Cancel any running jobs on the machine.
                    if let Some(running) = old_class.running {
                        self.cancel_job(running);
                    }

                    JobServerResp::Ok
                } else {
                    error!("No such machine: {}", addr);
                    JobServerResp::NoSuchMachine
                }
            }

            ListAvailable => JobServerResp::Machines(
                self.machines
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(addr, info)| (addr.clone(), info.class.clone()))
                    .collect(),
            ),

            ListVars => JobServerResp::Vars(self.variables.lock().unwrap().clone()),

            SetUpMachine { addr, class, cmds } => {
                let jid = self.next_jid.fetch_add(1, Ordering::Relaxed);

                info!(
                    "Create setup task with ID {}. Machine: {}. Cmds: {:?}",
                    jid, addr, cmds
                );

                let variables = self.variables.lock().unwrap().clone();

                self.tasks.lock().unwrap().insert(
                    jid,
                    Task {
                        jid,
                        ty: TaskType::SetupTask,
                        cmds,
                        class,
                        machine: Some(addr),
                        state: TaskState::Waiting,
                        variables,
                        cp_results: None,
                        canceled: false,
                    },
                );

                JobServerResp::JobId(jid)
            }

            SetVar { name, value } => {
                let old = self
                    .variables
                    .lock()
                    .unwrap()
                    .insert(name.clone(), value.clone());

                info!("Set {}={}", name, value);

                if let Some(old_value) = old {
                    warn!(
                        "Old value of {} was {}. New value is {}",
                        name, old_value, value
                    );
                }

                // Respond
                JobServerResp::Ok
            }

            AddJob {
                class,
                cmd,
                cp_results,
            } => {
                let jid = self.next_jid.fetch_add(1, Ordering::Relaxed);

                info!("Added job {} with class {}: {}", jid, class, cmd);

                let variables = self.variables.lock().unwrap().clone();

                self.tasks.lock().unwrap().insert(
                    jid,
                    Task {
                        jid,
                        ty: TaskType::Job,
                        cmds: vec![cmd],
                        class: Some(class),
                        cp_results,
                        state: TaskState::Waiting,
                        variables,
                        machine: None,
                        canceled: false,
                    },
                );

                JobServerResp::JobId(jid)
            }

            ListJobs => {
                let tasks: Vec<_> = self.tasks.lock().unwrap().keys().map(|&k| k).collect();
                JobServerResp::Jobs(tasks)
                // drop locks
            }

            CancelJob { jid } => self.cancel_job(jid),

            JobStatus { jid } => {
                let locked_tasks = self.tasks.lock().unwrap();
                let task = locked_tasks.get(&jid);
                match task {
                    Some(Task {
                        jid,
                        ty: TaskType::Job,
                        class,
                        cmds,
                        variables,
                        ..
                    }) => {
                        info!("Stating job {}, {:?}", jid, task);

                        JobServerResp::JobStatus {
                            jid: *jid,
                            class: class.as_ref().expect("No class for clone").clone(),
                            cmd: cmds.first().unwrap().clone(),
                            status: task.unwrap().status(),
                            variables: variables.clone(),
                        }
                    }

                    Some(Task {
                        jid,
                        ty: TaskType::SetupTask,
                        class,
                        cmds,
                        state,
                        variables,
                        ..
                    }) => {
                        info!("Stating setup task {}, {:?}", jid, task);

                        let cmd = match state {
                            TaskState::Waiting => &cmds[0],
                            TaskState::Running(idx) => &cmds[*idx],
                            TaskState::Done
                            | TaskState::DoneWithResults { .. }
                            | TaskState::Error { .. }
                            | TaskState::CheckingResults
                            | TaskState::CopyingResults { .. }
                            | TaskState::Finalize { .. }
                            | TaskState::Canceled => cmds.last().unwrap(),
                        }
                        .clone();

                        JobServerResp::JobStatus {
                            jid: *jid,
                            class: class.as_ref().map(Clone::clone).unwrap_or("".into()),
                            cmd,
                            status: task.unwrap().status(),
                            variables: variables.clone(),
                        }
                    }

                    None => {
                        error!("No such job: {}", jid);
                        JobServerResp::NoSuchJob
                    }
                }
            }

            CloneJob { jid } => {
                let mut locked_jobs = self.tasks.lock().unwrap();
                let task = locked_jobs.get(&jid);

                match task {
                    Some(Task {
                        jid,
                        ty: TaskType::Job,
                        cmds,
                        class,
                        variables,
                        cp_results,
                        ..
                    }) => {
                        let new_jid = self.next_jid.fetch_add(1, Ordering::Relaxed);

                        info!(
                            "Cloning job {} to job {}, {:?}",
                            jid,
                            new_jid,
                            task.unwrap()
                        );

                        let cmds = cmds.clone();
                        let class = class.clone();
                        let cp_results = cp_results.clone();
                        let variables = variables.clone();

                        locked_jobs.insert(
                            new_jid,
                            Task {
                                jid: new_jid,
                                ty: TaskType::Job,
                                cmds,
                                class,
                                cp_results,
                                variables,
                                state: TaskState::Waiting,
                                machine: None,
                                canceled: false,
                            },
                        );

                        JobServerResp::JobId(new_jid)
                    }

                    Some(Task {
                        jid,
                        ty: TaskType::SetupTask,
                        cmds,
                        class,
                        machine,
                        variables,
                        ..
                    }) => {
                        let new_jid = self.next_jid.fetch_add(1, Ordering::Relaxed);

                        info!(
                            "Cloning setup task {} to setup task {}, {:?}",
                            jid, new_jid, task
                        );

                        let cmds = cmds.clone();
                        let class = class.clone();
                        let machine = machine.clone();
                        let variables = variables.clone();

                        locked_jobs.insert(
                            new_jid,
                            Task {
                                jid: new_jid,
                                ty: TaskType::SetupTask,
                                cmds,
                                class,
                                machine,
                                cp_results: None,
                                state: TaskState::Waiting,
                                variables,
                                canceled: false,
                            },
                        );

                        JobServerResp::JobId(new_jid)
                    }

                    None => {
                        error!("No such job or setup task: {}", jid);
                        JobServerResp::NoSuchJob
                    }
                }
            }

            AddMatrix {
                mut vars,
                cmd,
                class,
                cp_results,
            } => {
                let id = self.next_jid.fetch_add(1, Ordering::Relaxed);

                // Get the set of base variables, some of which may be overridden by the matrix
                // variables in the template.
                vars.extend(
                    self.variables
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(k, v)| (k.to_owned(), vec![v.to_owned()])),
                );

                info!(
                    "Create matrix with ID {}. Cmd: {:?}, Vars: {:?}",
                    id, cmd, vars
                );

                let mut jids = vec![];

                // Create a new job for every element in the cartesian product of the variables.
                for config in jobserver::cartesian_product(&vars) {
                    let jid = self.next_jid.fetch_add(1, Ordering::Relaxed);
                    jids.push(jid);

                    let cmd = cmd_replace_vars(&cmd, &config);

                    info!(
                        "[Matrix {}] Added job {} with class {}: {}",
                        id, jid, class, cmd
                    );

                    self.tasks.lock().unwrap().insert(
                        jid,
                        Task {
                            jid,
                            ty: TaskType::Job,
                            cmds: vec![cmd],
                            class: Some(class.clone()),
                            cp_results: cp_results.clone(),
                            state: TaskState::Waiting,
                            variables: config,
                            machine: None,
                            canceled: false,
                        },
                    );
                }

                self.matrices.lock().unwrap().insert(
                    id,
                    Matrix {
                        id,
                        cmd,
                        class,
                        cp_results,
                        variables: vars,
                        jids,
                    },
                );

                JobServerResp::MatrixId(id)
            }

            StatMatrix { id } => {
                if let Some(matrix) = self.matrices.lock().unwrap().get(&id) {
                    info!("Stating matrix {}, {:?}", id, matrix);

                    JobServerResp::MatrixStatus {
                        id,
                        class: matrix.class.clone(),
                        cp_results: matrix.cp_results.clone(),
                        cmd: matrix.cmd.clone(),
                        jobs: matrix.jids.clone(),
                        variables: matrix.variables.clone(),
                    }
                } else {
                    error!("No such matrix: {}", id);
                    JobServerResp::NoSuchMatrix
                }
            }
        };

        Ok(response)
    }
}

impl Server {
    /// Mark the given job as canceled. This doesn't actually do anything yet. The job will be
    /// killed and removed asynchronously.
    fn cancel_job(&self, jid: usize) -> JobServerResp {
        // We set the `canceled` flag and let the job server handle the rest.

        if let Some(job) = self.tasks.lock().unwrap().get_mut(&jid) {
            info!("Cancelling task {}, {:?}", jid, job);
            job.canceled = true;
            JobServerResp::Ok
        } else {
            error!("No such job: {}", jid);
            JobServerResp::NoSuchJob
        }
    }
}

impl Server {
    /// Start the thread that actually gets stuff done...
    pub fn start_work_thread(self: Arc<Self>) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || Self::work_thread(self))
    }

    /// Does the actual work... attempts to drive each state machine forward if it can be. Then,
    /// sleeps a bit.
    fn work_thread(self: Arc<Self>) {
        let mut running_job_handles = HashMap::new();
        let mut to_remove = HashSet::new();
        let copying_flags = Arc::new(Mutex::new(HashSet::new()));

        loop {
            info!("Work thread iteration.");

            // We keep track of whether any task had a state change. If there was, then there is
            // increased chance of another state change soon, so iterate with higher frequency.
            let mut updated = false;

            {
                let mut locked_machines = self.machines.lock().unwrap();
                let mut locked_tasks = self.tasks.lock().unwrap();

                for (jid, task) in locked_tasks.iter_mut() {
                    updated = updated
                        || Self::try_drive_sm(
                            &self.runner,
                            *jid,
                            task,
                            &mut *locked_machines,
                            &mut running_job_handles,
                            &mut to_remove,
                            copying_flags.clone(),
                        );
                }

                // Remove any canceled processes from the task list.
                for jid in to_remove.drain() {
                    let _ = locked_tasks.remove(&jid);
                }

                // drop locks
            }

            // Sleep a bit.
            std::thread::sleep(if updated {
                Duration::from_millis(100)
            } else {
                Duration::from_secs(5)
            });
        }
    }

    /// Attempts to drive the given task's state machine forward one step.
    fn try_drive_sm(
        runner: &str,
        jid: usize,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        running_job_handles: &mut HashMap<usize, JobProcessInfo>,
        to_remove: &mut HashSet<usize>,
        copying_flags: Arc<Mutex<HashSet<usize>>>,
    ) -> bool {
        // If the task was canceled since the last time it was driven, set the state to `Canceled`
        // so it can get garbage collected.
        if task.canceled {
            task.state = TaskState::Canceled;
        }

        match task.state {
            TaskState::Waiting => {
                Self::try_drive_sm_waiting(runner, jid, task, machines, running_job_handles)
            }

            TaskState::Running(idx) => {
                Self::try_drive_sm_running(runner, jid, task, machines, running_job_handles, idx)
            }

            TaskState::CheckingResults => Self::try_drive_sm_checking_results(
                jid,
                task,
                machines,
                running_job_handles,
                copying_flags,
            ),

            TaskState::CopyingResults { ref results_path } => {
                // The copy thread will insert a `()` just before it exits, indicating it's done.
                if copying_flags.lock().unwrap().remove(&jid) {
                    task.state = TaskState::Finalize {
                        results_path: Some(results_path.clone()),
                    };
                    true
                } else {
                    false
                }
            }

            TaskState::Finalize { ref results_path } => {
                let results_path = results_path.clone();

                Self::try_drive_sm_finalize(jid, task, machines, running_job_handles, results_path)
            }
            TaskState::Done | TaskState::DoneWithResults { .. } => {
                let old = running_job_handles.remove(&jid);

                // Update occured if we actually removed something.
                old.is_some()
            }

            TaskState::Error { .. } => {
                // Free any associated machine.
                let machine = task.machine.as_ref().unwrap();
                if let Some(machine) = machines.get_mut(machine) {
                    machine.running = None;
                }

                // Clean up job handles
                let _ = running_job_handles.remove(&jid);

                true
            }

            TaskState::Canceled => {
                Self::try_drive_sm_canceled(jid, task, machines, running_job_handles, to_remove)
            }
        }
    }

    /// The task has not started yet. Attempt to do so now.
    fn try_drive_sm_waiting(
        runner: &str,
        jid: usize,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        running_job_handles: &mut HashMap<usize, JobProcessInfo>,
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
            task.state = TaskState::Running(0);
            task.machine = Some(machine.clone());

            match Self::run_cmd(jid, task, &machine, runner) {
                Ok(job) => {
                    info!("Running job {} on machine {}", jid, machine);

                    running_job_handles.insert(jid, job);
                }
                Err(err) => {
                    error!("Unable to start job {}: {}", jid, err);

                    task.state = TaskState::Error {
                        error: format!("Unable to start job {}: {}", jid, err),
                    };
                }
            };

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
        jid: usize,
        task: &mut Task,
        _machines: &mut HashMap<String, MachineStatus>,
        running_job_handles: &mut HashMap<usize, JobProcessInfo>,
        idx: usize,
    ) -> bool {
        let job_proc_info = running_job_handles
            .get_mut(&jid)
            .expect("No job process info for running command.");

        let machine = task.machine.as_ref().unwrap();

        match job_proc_info.handle.try_wait() {
            // Job terminated.
            Ok(Some(status)) => {
                info!("Task {} terminated with status code {}.", jid, status);

                // If status code indicates sucess, then proceed. Otherwise, fail the task.
                if status.success() {
                    // If this is the last command of the task, then we are done.
                    // Otherwise, start the next command.
                    if idx == task.cmds.len() - 1 {
                        info!("Task {} is complete. Need to check for results.", jid);
                        task.state = TaskState::CheckingResults;
                    } else {
                        info!("Starting cmd {} of task {}.", idx + 1, jid);

                        task.state = TaskState::Running(idx + 1);

                        // Actually start the command now.
                        match Self::run_cmd(jid, task, machine, runner) {
                            Ok(job) => {
                                info!("Running job {} on machine {}", jid, machine);
                                running_job_handles.insert(jid, job);
                            }
                            Err(err) => {
                                error!("Unable to start job {}: {}", jid, err);

                                task.state = TaskState::Error {
                                    error: format!("Unable to start job {}: {}", jid, err),
                                };
                            }
                        };
                    }
                } else {
                    task.state = TaskState::Error {
                        error: format!("Task returned failing exit code: {}", status),
                    };
                }

                true
            }

            // Not ready yet... do nothing.
            Ok(None) => false,

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
        jid: usize,
        task: &mut Task,
        _machines: &mut HashMap<String, MachineStatus>,
        _running_job_handles: &mut HashMap<usize, JobProcessInfo>,
        copying_flags: Arc<Mutex<HashSet<usize>>>,
    ) -> bool {
        // Look through the stdout for the "RESULTS: " line to get the results path.
        let results_path = {
            let cmd = task.cmds.last().unwrap();
            let stdout_file_name = format!("{}", cmd_to_path(jid, &cmd));
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
                {
                    let machine = task.machine.as_ref().unwrap().clone();
                    let cp_results = cp_results.clone();
                    let results_path = results_path.clone();

                    std::thread::spawn(move || {
                        // Copy via SCP
                        info!("Copying results (job {}) to {}", jid, cp_results);

                        // HACK: assume all machine names are in the form HOSTNAME:PORT, and all
                        // results are output to `vm_shared/results/` on the remote.
                        let machine_ip = machine.split(":").next().unwrap();

                        let scp_result = std::process::Command::new("scp")
                            .arg(&format!(
                                "{}:vm_shared/results/{}", // TODO: make this generic
                                machine_ip, results_path
                            ))
                            .arg(cp_results)
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .output();

                        match scp_result {
                            Ok(..) => info!("Finished copying results for job {}.", jid),
                            Err(e) => error!("Error copy results for job {}: {}", jid, e),
                        }

                        // Indicate we are done.
                        copying_flags.lock().unwrap().insert(jid);
                    });
                }

                task.state = TaskState::CopyingResults {
                    results_path: results_path.clone(),
                };
            }

            (Some(_), None) => {
                warn!("Task {} expected results, but none were produced.", jid);

                task.state = TaskState::Finalize { results_path: None };
            }

            (None, Some(_)) => {
                warn!(
                    "Discarding results for task {} even though they were produced.",
                    jid
                );

                task.state = TaskState::Finalize { results_path: None };
            }

            (None, None) => {
                task.state = TaskState::Finalize { results_path: None };
            }
        }

        true
    }

    fn try_drive_sm_finalize(
        jid: usize,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        _running_job_handles: &mut HashMap<usize, JobProcessInfo>,
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

                // Add machine
                machines.insert(
                    machine.clone(),
                    MachineStatus {
                        class: class.clone(),
                        running: running_job,
                    },
                );
            }

            (TaskType::Job, _) => {
                info!("Releasing machine {:?}", machine);
                machines
                    .get_mut(machine)
                    .expect("Job running on non-existant machine.")
                    .running = None;
            }

            _ => {}
        }

        // Move to a Done state.
        if results_path.is_some() {
            task.state = TaskState::DoneWithResults {
                results_path: results_path.unwrap(),
            };
        } else {
            task.state = TaskState::Done;
        }

        true
    }

    fn try_drive_sm_canceled(
        jid: usize,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        running_job_handles: &mut HashMap<usize, JobProcessInfo>,
        to_remove: &mut HashSet<usize>,
    ) -> bool {
        // Kill any running processes
        if let Some(mut handle) = running_job_handles.remove(&jid) {
            let _ = handle.handle.kill(); // SIGKILL
        }

        // Free any associated machine.
        let machine = task.machine.as_ref().unwrap();
        if let Some(machine) = machines.get_mut(machine) {
            machine.running = None;
        }

        // Add to the list to be reaped.
        to_remove.insert(jid);

        true
    }

    fn run_cmd(
        jid: usize,
        task: &Task,
        machine: &str,
        runner: &str,
    ) -> std::io::Result<JobProcessInfo> {
        let cmd_idx = match task.state {
            TaskState::Running(idx) => idx,
            _ => unreachable!(),
        };
        let cmd = &task.cmds[cmd_idx];
        let cmd = cmd_replace_machine(&cmd_replace_vars(&cmd, &task.variables), &machine);

        // The job will output stdout and stderr to these files. When the job completes, we will
        // search through the stdout file to get the name of the results file to copy, if any.

        let stdout_file_name = format!("{}", cmd_to_path(jid, &cmd));
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
}

fn main() {
    let matches = clap_app! { jobserver =>
        (about: "Serves jobs to machines")
        (@arg RUNNER: +required
         "Path to the runner binary. This binary is provided with \
         the arguments of the submitted job command.")
        (@arg LOGGING_CONFIG: +required
         "Path to the log4rs config file")
        (@arg ADDR: --addr +takes_value
         "The IP:ADDR for the server to listen on for commands \
         (defaults to `localhost:3030`)")
    }
    .get_matches();

    let addr = matches.value_of("ADDR").unwrap_or(SERVER_ADDR.into());
    let logging_config = matches.value_of("LOGGING_CONFIG").unwrap();
    let runner = matches.value_of("RUNNER").unwrap();

    // Start logger
    log4rs::init_file(logging_config, Default::default()).expect("Unable to init logger");

    info!("Starting server at {}", addr);

    // Listen for client requests on the main thread, while we do work in the background.
    let server = Arc::new(Server::new(runner.into()));
    Arc::clone(&server).start_work_thread();
    server.listen(addr);
}
