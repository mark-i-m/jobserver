//! Server implmentation

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use clap::clap_app;

use jobserver::{
    cmd_replace_machine, cmd_replace_vars, cmd_to_path,
    protocol::{
        self,
        request::RequestType::{self, *},
        response::ResponseType::{self, *},
    },
    SERVER_ADDR,
};

use log::{debug, error, info, warn};

use prost::Message;

mod snapshot;

/// The name of the file in the `log_dir` that a snapshot is stored at.
const DUMP_FILENAME: &str = "server-snapshot";

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
    tasks: Arc<Mutex<BTreeMap<u64, Task>>>,

    /// Information about matrices, by ID.
    matrices: Arc<Mutex<HashMap<u64, Matrix>>>,

    /// The next job ID to be assigned.
    next_jid: AtomicU64,

    /// The path to the runner. Never changes.
    runner: String,

    /// The directory into which to write job logs.
    log_dir: String,

    /// Set to true when a client does something. This is mainly optimization to inform whether
    /// the worker thread might want to check for some work.
    client_ping: Arc<AtomicBool>,
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

    /// This task has not yet started running and is on hold.
    Held,

    /// This task is running the `n`th command in the `cmds` vector.
    Running(usize),

    /// The task terminated with a successful error code, but we have not yet checked for results.
    CheckingResults,

    /// The task completed and we are copying results.
    CopyingResults { results_path: String },

    /// Everything is done, and we are about to move to a Done state.
    Finalize { results_path: Option<String> },

    /// This task was canceled. If `remove` is true, then it will be garbage collected.
    Canceled { remove: bool },

    /// This task has completed.
    Done,

    /// This task has completed and results were produced.
    DoneWithResults {
        /// The filename (prefix) of results from the task.
        results_path: String,
    },

    /// This task terminated with an error, but we still need to do cleanup. `n` represents to
    /// command index of the failed task.
    Error { error: String, n: usize },

    /// This task terminated with an error (and cleanup has already been done). `n` represents to
    /// command index of the failed task.
    ErrorDone { error: String, n: usize },

    /// This task was killed/canceled, but not garbage collected.
    Killed,

    /// This task is in an unknown state.
    Unknown { machine: Option<String> },
}

/// Information about a single task (a job or setup task). This is a big state machine that says
/// what the status of the task is and has all information to do the next thing when ready.
#[derive(Clone, Debug)]
struct Task {
    /// The tasks's ID.
    jid: u64,

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

    /// Set to `Some` if the task is canceled; if `Some(true)` the task should be gc'ed.
    canceled: Option<bool>,

    /// The state of the task that we are currently in. This defines a state machine for the task.
    state: TaskState,

    /// If true, then automatically clone the job if it fails.
    repeat_on_fail: bool,
}

/// A collection of jobs that run over the cartesian product of some set of variables.
#[derive(Clone, Debug)]
struct Matrix {
    /// This matrix's ID.
    id: u64,

    /// The command (without replacements).
    cmd: String,

    /// The class of the machines that can run this job.
    class: String,

    /// The location to copy results, if any.
    cp_results: Option<String>,

    /// The variables and their possible values.
    variables: HashMap<String, Vec<String>>,

    /// A list of jobs in this matrix.
    jids: Vec<u64>,
}

/// Information about a single machine.
#[derive(Debug, PartialEq, Clone, Eq, Hash)]
struct MachineStatus {
    /// The class of the machine.
    class: String,

    /// What job it is running, if any.
    running: Option<u64>,
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
    pub fn status(&self) -> protocol::Status {
        use protocol::status::{Erroropt::Error, Machineopt::Machine, Outputopt::Output};

        let mut status = protocol::Status::default();

        // Set the state
        match &self.state {
            TaskState::Waiting => {
                status.status = protocol::status::Status::Waiting.into();
            }

            TaskState::Held => {
                status.status = protocol::status::Status::Held.into();
            }

            TaskState::Running(..)
            | TaskState::CheckingResults { .. }
            | TaskState::Finalize { .. } => {
                status.status = protocol::status::Status::Running.into();
            }

            TaskState::CopyingResults { .. } => {
                status.status = protocol::status::Status::Copyresults.into();
            }

            TaskState::Done | TaskState::DoneWithResults { .. } => {
                status.status = protocol::status::Status::Done.into();
            }

            TaskState::Error { .. } | TaskState::ErrorDone { .. } => {
                status.status = protocol::status::Status::Failed.into();
            }

            TaskState::Canceled { .. } | TaskState::Killed => {
                status.status = protocol::status::Status::Canceled.into();
            }

            TaskState::Unknown { .. } => {
                status.status = protocol::status::Status::Unknown.into();
            }
        }

        // Set the machine
        match &self.state {
            TaskState::Running(..)
            | TaskState::CheckingResults { .. }
            | TaskState::Finalize { .. }
            | TaskState::CopyingResults { .. }
            | TaskState::Done
            | TaskState::DoneWithResults { .. } => {
                status.machineopt = Some(Machine(self.machine.as_ref().unwrap().clone()));
            }

            TaskState::Error { .. } | TaskState::ErrorDone { .. } => {
                status.machineopt = Some(Machine(self.machine.as_ref().unwrap().clone()));
            }

            TaskState::Unknown { machine } => {
                status.machineopt = machine.clone().map(|m| Machine(m));
            }

            TaskState::Waiting
            | TaskState::Held
            | TaskState::Canceled { .. }
            | TaskState::Killed => {}
        }

        // Set the output
        match &self.state {
            TaskState::DoneWithResults { results_path } => {
                status.outputopt = Some(Output(results_path.clone()));
            }

            TaskState::Running { .. }
            | TaskState::CheckingResults
            | TaskState::CopyingResults { .. }
            | TaskState::Finalize { .. }
            | TaskState::Done
            | TaskState::Error { .. }
            | TaskState::ErrorDone { .. }
            | TaskState::Waiting
            | TaskState::Held
            | TaskState::Canceled { .. }
            | TaskState::Killed
            | TaskState::Unknown { .. } => {}
        }

        // Set the error
        match &self.state {
            TaskState::Error { error, .. } | TaskState::ErrorDone { error, .. } => {
                status.erroropt = Some(Error(error.clone()));
            }

            TaskState::Running { .. }
            | TaskState::CheckingResults
            | TaskState::CopyingResults { .. }
            | TaskState::Finalize { .. }
            | TaskState::Done
            | TaskState::DoneWithResults { .. }
            | TaskState::Waiting
            | TaskState::Held
            | TaskState::Canceled { .. }
            | TaskState::Killed
            | TaskState::Unknown { .. } => {}
        }

        status
    }

    pub fn update_state(&mut self, new: TaskState) {
        info!(
            "Task {} SM Update\nOLD: {:?}\nNEW: {:?}",
            self.jid, self.state, new
        );
        self.state = new;
    }
}

impl Server {
    /// Creates a new server. Not listening yet.
    pub fn new(runner: String, log_dir: String, allow_snap_fail: bool) -> Self {
        let mut server = Self {
            machines: Arc::new(Mutex::new(HashMap::new())),
            variables: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(BTreeMap::new())),
            matrices: Arc::new(Mutex::new(HashMap::new())),
            next_jid: AtomicU64::new(0),
            runner,
            log_dir,
            client_ping: Arc::new(AtomicBool::new(false)),
        };

        let is_loaded = server.load_snapshot();

        if !allow_snap_fail && !is_loaded {
            error!("Unable to load snapshot. Aborting.");
            std::process::exit(1);
        }

        server
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
        // Indicate that the work thread should check for new tasks.
        self.client_ping.fetch_or(true, Ordering::Relaxed);

        let peer_addr = client.peer_addr()?;
        info!("Handling request from {}", peer_addr);

        let mut request = Vec::new();
        client.read_to_end(&mut request)?;

        let request = protocol::Request::decode(request.as_slice())?;

        info!("(request) {}: {:?}", peer_addr, request);

        client.shutdown(Shutdown::Read)?;

        let request = match request.request_type {
            Some(request) => request,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Request was unexpectedly empty.",
                ))
            }
        };

        let response_ty = self.handle_request(request)?;

        info!("(response) {}: {:?}", peer_addr, response_ty);

        let mut response = protocol::Response::default();
        response.response_type = Some(response_ty);
        let mut bytes = vec![];
        response.encode(&mut bytes)?;

        client.write_all(&bytes)?;

        Ok(())
    }

    fn handle_request(&self, request: RequestType) -> std::io::Result<ResponseType> {
        let response = match request {
            Preq(protocol::PingRequest {}) => Okresp(protocol::OkResp {}),

            Mareq(protocol::MakeAvailableRequest { addr, class }) => {
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
                Okresp(protocol::OkResp {})
            }

            Rareq(protocol::RemoveAvailableRequest { addr }) => {
                if let Some(old_class) = self.machines.lock().unwrap().remove(&addr) {
                    info!("Removed machine {}/{}", addr, old_class.class);

                    // Cancel any running jobs on the machine.
                    if let Some(running) = old_class.running {
                        self.cancel_job(running, false);
                    }

                    Okresp(protocol::OkResp {})
                } else {
                    error!("No such machine: {}", addr);
                    Nsmresp(protocol::NoSuchMachineResp {})
                }
            }

            Lareq(protocol::ListAvailableRequest {}) => Mresp(protocol::MachinesResp {
                machines: self
                    .machines
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(addr, info)| (addr.clone(), info.class.clone()))
                    .collect(),
            }),

            Lvreq(protocol::ListVarsRequest {}) => Vresp(protocol::VarsResp {
                vars: self.variables.lock().unwrap().clone(),
            }),

            Sumreq(protocol::SetUpMachineRequest {
                addr,
                classopt,
                cmds,
            }) => {
                let jid = self.next_jid.fetch_add(1, Ordering::Relaxed);

                info!(
                    "Create setup task with ID {}. Machine: {}. Cmds: {:?}",
                    jid, addr, cmds
                );

                let variables = self.variables.lock().unwrap().clone();
                let class =
                    classopt.map(|protocol::set_up_machine_request::Classopt::Class(class)| class);

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
                        canceled: None,
                        repeat_on_fail: false,
                    },
                );

                Jiresp(protocol::JobIdResp { jid })
            }

            Svreq(protocol::SetVarRequest { name, value }) => {
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
                Okresp(protocol::OkResp {})
            }

            Ajreq(protocol::AddJobRequest {
                class,
                cmd,
                cp_resultsopt,
                // prost uses a default of `false`.
                repeat_on_fail,
            }) => {
                let jid = self.next_jid.fetch_add(1, Ordering::Relaxed);

                info!("Added job {} with class {}: {}", jid, class, cmd);

                let variables = self.variables.lock().unwrap().clone();
                let cp_results =
                    cp_resultsopt.map(|protocol::add_job_request::CpResultsopt::CpResults(s)| s);

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
                        canceled: None,
                        repeat_on_fail,
                    },
                );

                Jiresp(protocol::JobIdResp { jid })
            }

            Ljreq(protocol::ListJobsRequest {}) => {
                let tasks: Vec<_> = self.tasks.lock().unwrap().keys().map(|&k| k).collect();
                Jresp(protocol::JobsResp { jobs: tasks })
                // drop locks
            }

            Hjreq(protocol::HoldJobRequest { jid }) => {
                let mut locked_tasks = self.tasks.lock().unwrap();
                let task = locked_tasks.get_mut(&jid);

                match task {
                    Some(task) => {
                        let is_waiting = match task.state {
                            TaskState::Waiting | TaskState::Held => true,
                            _ => false,
                        };

                        if is_waiting {
                            task.state = TaskState::Held;
                            Okresp(protocol::OkResp {})
                        } else {
                            error!(
                                "Attempted to put task {} on hold, but current state is {:?}",
                                jid, task.state
                            );
                            Nwresp(protocol::NotWaitingResp {})
                        }
                    }

                    None => Nsjresp(protocol::NoSuchJobResp {}),
                }
            }

            Ujreq(protocol::UnholdJobRequest { jid }) => {
                let mut locked_tasks = self.tasks.lock().unwrap();
                let task = locked_tasks.get_mut(&jid);

                match task {
                    Some(task) => {
                        let is_held = match task.state {
                            TaskState::Held => true,
                            _ => false,
                        };

                        if is_held {
                            task.state = TaskState::Waiting;
                            Okresp(protocol::OkResp {})
                        } else {
                            error!(
                                "Attempted to unhold task {}, but current state is {:?}",
                                jid, task.state
                            );
                            Nwresp(protocol::NotWaitingResp {})
                        }
                    }

                    None => Nsjresp(protocol::NoSuchJobResp {}),
                }
            }

            Cjreq(protocol::CancelJobRequest { jid, remove }) => self.cancel_job(jid, remove),

            Jsreq(protocol::JobStatusRequest { jid }) => {
                let locked_tasks = self.tasks.lock().unwrap();
                let task = locked_tasks.get(&jid);
                match task {
                    Some(Task {
                        jid,
                        ty: TaskType::Job,
                        class,
                        cmds,
                        variables,
                        machine,
                        ..
                    }) => {
                        info!("Status of job {}, {:?}", jid, task);

                        let log = if let Some(machine) = machine {
                            let cmd = cmd_replace_machine(
                                &cmd_replace_vars(cmds.first().unwrap(), &variables),
                                &machine,
                            );
                            format!("{}", cmd_to_path(*jid, &cmd, &self.log_dir))
                        } else {
                            "/dev/null".into()
                        };

                        Jsresp(protocol::JobStatusResp {
                            jid: *jid,
                            class: class.as_ref().expect("No class for clone").clone(),
                            cmd: cmds.first().unwrap().clone(),
                            status: Some(task.unwrap().status()),
                            variables: variables.clone(),
                            log,
                        })
                    }

                    Some(Task {
                        jid,
                        ty: TaskType::SetupTask,
                        class,
                        cmds,
                        state,
                        variables,
                        machine,
                        ..
                    }) => {
                        info!("Status setup task {}, {:?}", jid, task);

                        let cmd = match state {
                            TaskState::Waiting | TaskState::Held => &cmds[0],
                            TaskState::Running(idx) => &cmds[*idx],
                            TaskState::Error { n, .. } | TaskState::ErrorDone { n, .. } => {
                                &cmds[*n]
                            }
                            TaskState::Done
                            | TaskState::DoneWithResults { .. }
                            | TaskState::CheckingResults
                            | TaskState::CopyingResults { .. }
                            | TaskState::Finalize { .. }
                            | TaskState::Canceled { .. }
                            | TaskState::Killed
                            | TaskState::Unknown { .. } => cmds.last().unwrap(),
                        }
                        .clone();

                        let log = if let Some(machine) = machine {
                            let cmd =
                                cmd_replace_machine(&cmd_replace_vars(&cmd, &variables), &machine);
                            format!("{}", cmd_to_path(*jid, &cmd, &self.log_dir))
                        } else {
                            "/dev/null".into()
                        };

                        Jsresp(protocol::JobStatusResp {
                            jid: *jid,
                            class: class.as_ref().map(Clone::clone).unwrap_or("".into()),
                            cmd,
                            status: Some(task.unwrap().status()),
                            variables: variables.clone(),
                            log,
                        })
                    }

                    None => {
                        error!("No such job: {}", jid);
                        Nsjresp(protocol::NoSuchJobResp {})
                    }
                }
            }

            Cljreq(protocol::CloneJobRequest { jid }) => {
                let mut locked_jobs = self.tasks.lock().unwrap();
                let task = locked_jobs.get(&jid);

                match task {
                    Some(
                        task
                        @
                        Task {
                            ty: TaskType::Job, ..
                        },
                    )
                    | Some(
                        task
                        @
                        Task {
                            ty: TaskType::SetupTask,
                            cp_results: None,
                            repeat_on_fail: false,
                            ..
                        },
                    ) => {
                        let new_jid = self.next_jid.fetch_add(1, Ordering::Relaxed);
                        let task = Self::clone_task(new_jid, task);

                        locked_jobs.insert(new_jid, task);

                        Jiresp(protocol::JobIdResp { jid })
                    }

                    None => {
                        error!("No such job or setup task: {}", jid);
                        Nsjresp(protocol::NoSuchJobResp {})
                    }

                    weird_state => {
                        error!(
                            "Unexpected task state! Ignoring clone request. {:#?}",
                            weird_state
                        );
                        Ierr(protocol::InternalError {})
                    }
                }
            }

            Amreq(protocol::AddMatrixRequest {
                vars,
                cmd,
                class,
                cp_resultsopt,
            }) => {
                let id = self.next_jid.fetch_add(1, Ordering::Relaxed);

                let mut vars = protocol::reverse_map(&vars);
                let cp_results =
                    cp_resultsopt.map(|protocol::add_matrix_request::CpResultsopt::CpResults(s)| s);

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
                            canceled: None,
                            repeat_on_fail: false,
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

                Miresp(protocol::MatrixIdResp { id })
            }

            Smreq(protocol::StatMatrixRequest { id }) => {
                if let Some(matrix) = self.matrices.lock().unwrap().get(&id) {
                    info!("Status of matrix {}, {:?}", id, matrix);

                    let cp_resultsopt = matrix
                        .cp_results
                        .as_ref()
                        .map(|s| protocol::matrix_status_resp::CpResultsopt::CpResults(s.into()));

                    Msresp(protocol::MatrixStatusResp {
                        id,
                        class: matrix.class.clone(),
                        cp_resultsopt,
                        cmd: matrix.cmd.clone(),
                        jobs: matrix.jids.clone(),
                        variables: protocol::convert_map(&matrix.variables),
                    })
                } else {
                    error!("No such matrix: {}", id);
                    Nsmatresp(protocol::NoSuchMatrixResp {})
                }
            }
        };

        Ok(response)
    }
}

impl Server {
    /// Mark the given job as canceled. This doesn't actually do anything yet. The job will be
    /// killed and removed asynchronously.
    fn cancel_job(&self, jid: u64, remove: bool) -> ResponseType {
        // We set the `canceled` flag and let the job server handle the rest.

        if let Some(job) = self.tasks.lock().unwrap().get_mut(&jid) {
            info!("Cancelling task {}, {:?}", jid, job);
            job.canceled = Some(remove);
            Okresp(protocol::OkResp {})
        } else {
            error!("No such job: {}", jid);
            Nsjresp(protocol::NoSuchJobResp {})
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
        let mut to_clone = HashSet::new(); // failed tasks we need to clone
        let copying_flags = Arc::new(Mutex::new(HashSet::new()));

        // We keep track of whether any task had a state change. If there was, then there is
        // increased chance of another state change soon, so iterate with higher frequency.
        let mut updated = true;

        loop {
            // Write out state for later (before we grab any locks).
            self.take_snapshot();

            // Sleep 30s, but keep waking up to check if there is likely more work. If there is an
            // indication of potential work, do a full iteration.
            for _ in 0..30000 / 100 {
                std::thread::sleep(Duration::from_millis(100));

                if updated || self.client_ping.fetch_and(false, Ordering::Relaxed) {
                    break;
                }
            }

            updated = false;

            debug!("Work thread iteration.");

            {
                let mut locked_machines = self.machines.lock().unwrap();
                let mut locked_tasks = self.tasks.lock().unwrap();

                debug!("Machine stata: {:?}", *locked_machines);

                for (jid, task) in locked_tasks.iter_mut() {
                    updated = updated
                        || Self::try_drive_sm(
                            &self.runner,
                            &self.log_dir,
                            *jid,
                            task,
                            &mut *locked_machines,
                            &mut running_job_handles,
                            &mut to_remove,
                            &mut to_clone,
                            copying_flags.clone(),
                        );
                }

                debug!(
                    "Finished driving SMs. Have {} tasks to reap.",
                    to_remove.len()
                );

                // Remove any canceled processes from the task list.
                for jid in to_remove.drain() {
                    let _ = locked_tasks.remove(&jid);
                }

                // Clone any failed jobs that need to be cloned.
                for jid in to_clone.drain() {
                    let new_jid = self.next_jid.fetch_add(1, Ordering::Relaxed);
                    // unwrap ok, already checked in `try_drive_sm`
                    let old_task = locked_tasks.get(&jid).unwrap();
                    let task = Self::clone_task(new_jid, old_task);

                    locked_tasks.insert(new_jid, task);
                }

                // drop locks
            }

            debug!(
                "Work thread iteration done (updated={:?}). Sleeping.",
                updated
            );
        }
    }

    /// Clone the given task with the given jid. Return the new `Task`. It is the responsibility of
    /// the caller to actually add the new `Task` to the list of running tasks.
    fn clone_task(new_jid: u64, task: &Task) -> Task {
        match task {
            Task {
                jid,
                ty: TaskType::Job,
                cmds,
                class,
                variables,
                cp_results,
                state: _,
                machine: _,
                canceled: _,
                repeat_on_fail,
            } => {
                info!("Cloning job {} to job {}, {:?}", jid, new_jid, task);

                let cmds = cmds.clone();
                let class = class.clone();
                let cp_results = cp_results.clone();
                let variables = variables.clone();

                Task {
                    jid: new_jid,
                    ty: TaskType::Job,
                    cmds,
                    class,
                    cp_results,
                    variables,
                    state: TaskState::Waiting,
                    machine: None,
                    canceled: None,
                    repeat_on_fail: *repeat_on_fail,
                }
            }

            Task {
                jid,
                ty: TaskType::SetupTask,
                cmds,
                class,
                machine,
                variables,
                canceled: _,
                state: _,
                cp_results: None,
                repeat_on_fail: false,
            } => {
                info!(
                    "Cloning setup task {} to setup task {}, {:?}",
                    jid, new_jid, task
                );

                let cmds = cmds.clone();
                let class = class.clone();
                let machine = machine.clone();
                let variables = variables.clone();

                Task {
                    jid: new_jid,
                    ty: TaskType::SetupTask,
                    cmds,
                    class,
                    machine,
                    cp_results: None,
                    state: TaskState::Waiting,
                    variables,
                    canceled: None,
                    repeat_on_fail: false,
                }
            }

            _ => unreachable!(),
        }
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
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// Attempts to drive the given task's state machine forward one step.
    fn try_drive_sm(
        runner: &str,
        log_dir: &str,
        jid: u64,
        task: &mut Task,
        machines: &mut HashMap<String, MachineStatus>,
        running_job_handles: &mut HashMap<u64, JobProcessInfo>,
        to_remove: &mut HashSet<u64>,
        to_clone: &mut HashSet<u64>,
        copying_flags: Arc<Mutex<HashSet<u64>>>,
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

            TaskState::Running(idx) => Self::try_drive_sm_running(
                runner,
                log_dir,
                jid,
                task,
                machines,
                running_job_handles,
                idx,
            ),

            TaskState::CheckingResults => Self::try_drive_sm_checking_results(
                jid,
                task,
                machines,
                running_job_handles,
                copying_flags,
                log_dir,
            ),

            TaskState::CopyingResults { ref results_path } => {
                // The copy thread will insert a `()` just before it exits, indicating it's done.
                if copying_flags.lock().unwrap().remove(&jid) {
                    debug!("Copy results for task {} completed.", jid);
                    let results_path = results_path.clone();
                    task.update_state(TaskState::Finalize {
                        results_path: Some(results_path),
                    });
                    true
                } else {
                    false
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

                // Update occured if we actually removed something.
                old.is_some()
            }

            TaskState::Error { n, ref error } => {
                // Free any associated machine.
                Self::free_machine(jid, task, machines);

                // Clean up job handles
                let _ = running_job_handles.remove(&jid).is_some();

                // If this task is marked `repeat_on_fail`, then we will need to clone the task,
                // but we can't do so here because it creates a lot of borrow checker errors (due
                // to lots of things already being borrowed).
                if task.repeat_on_fail {
                    to_clone.insert(jid);
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
            task.update_state(TaskState::Running(0));
            task.machine = Some(machine.clone());

            match Self::run_cmd(jid, task, &machine, runner, log_dir) {
                Ok(job) => {
                    info!("Running job {} on machine {}", jid, machine);

                    running_job_handles.insert(jid, job);
                }
                Err(err) => {
                    error!("Unable to start job {}: {}", jid, err);

                    task.update_state(TaskState::Error {
                        error: format!("Unable to start job {}: {}", jid, err),
                        n: 0, // first cmd failed
                    });
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
                        task.update_state(TaskState::Running(idx + 1));

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
                            }
                        };
                    }
                } else {
                    task.update_state(TaskState::Error {
                        error: format!(
                            "Task (command {}) returned failing exit code: {}",
                            idx, status
                        ),
                        n: idx,
                    });
                }

                true
            }

            // Not ready yet... do nothing.
            Ok(None) => {
                debug!("Task {} is still running", jid);
                false
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
        _machines: &mut HashMap<String, MachineStatus>,
        _running_job_handles: &mut HashMap<u64, JobProcessInfo>,
        copying_flags: Arc<Mutex<HashSet<u64>>>,
        log_dir: &str,
    ) -> bool {
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
                {
                    let machine = task.machine.as_ref().unwrap().clone();
                    let cp_results = cp_results.clone();
                    let results_path = results_path.clone();

                    std::thread::spawn(move || {
                        // Copy via SCP
                        info!("Copying results (job {}) to {}", jid, cp_results);

                        // HACK: assume all machine names are in the form HOSTNAME:PORT.
                        let machine_ip = machine.split(":").next().unwrap();

                        let scp_result = std::process::Command::new("scp")
                            .arg(&format!("{}:{}", machine_ip, results_path))
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
            task.update_state(TaskState::Killed);
            task.canceled = None; // No need to cancel it again!
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
            TaskState::Running(idx) => idx,
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
}

fn main() {
    let matches = clap_app! { jobserver =>
        (about: "Serves jobs to machines")
        (@arg RUNNER: +required
         "Path to the runner binary. This binary is provided with \
         the arguments of the submitted job command.")
        (@arg LOG_DIR: +required
         "This is the directory in which to write job logs.")
        (@arg LOGGING_CONFIG: +required
         "Path to the log4rs config file")
        (@arg ADDR: --address +takes_value
         "The IP:ADDR for the server to listen on for commands \
         (defaults to `localhost:3030`)")
        (@arg NOSNAP: --allow_snap_fail
         "Allow snapshot loading to fail. You will need this the first \
          time you run the server.")
    }
    .get_matches();

    let addr = matches.value_of("ADDR").unwrap_or(SERVER_ADDR.into());
    let logging_config = matches.value_of("LOGGING_CONFIG").unwrap();
    let runner = matches.value_of("RUNNER").unwrap();
    let log_dir = matches.value_of("LOG_DIR").unwrap();
    let allow_snap_fail = matches.is_present("NOSNAP");

    // Set the RUST_BACKTRACE environment variable so that we always get backtraces. Normally, one
    // doesn't want this because of the performance penalty, but in this case, we don't care too
    // much, whereas the debugging improvement is massive.
    std::env::set_var("RUST_BACKTRACE", "1");

    // Start logger
    log4rs::init_file(logging_config, Default::default()).expect("Unable to init logger");

    info!("Starting server at {}", addr);

    // Listen for client requests on the main thread, while we do work in the background.
    let server = Arc::new(Server::new(runner.into(), log_dir.into(), allow_snap_fail));
    Arc::clone(&server).start_work_thread();
    server.listen(addr);
}
