//! Server implmentation

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::TcpListener;
use std::process::{Child, Command};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};

use chrono::{offset::Utc, DateTime, Duration};

use clap::clap_app;

use expjobserver::SERVER_ADDR;

use log::{debug, error, info};

use prost::Message;

mod copier;
mod request;
mod sm;
mod snapshot;

/// The name of the file in the `log_dir` that a snapshot is stored at.
const DUMP_FILENAME: &str = "server-snapshot";

/// The server's state.
#[derive(Debug)]
struct Server {
    // Lock ordering:
    // - machines
    // - tasks
    // - matrices
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

    /// Thist task is currently running.
    Running {
        /// The index of the command in the `cmds` vector.
        index: usize,
    },

    /// The task terminated with a successful error code, but we have not yet checked for results.
    CheckingResults,

    /// The task completed and we are copying results.
    CopyingResults { results_path: String },

    /// Everything is done, and we are about to move to a Done state.
    Finalize { results_path: Option<String> },

    /// This task is in the process of being canceled. If `remove` is true, then it will be garbage
    /// collected. Otherwise, it will simply move to the `Killed` state.
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
    /// The task's ID.
    jid: u64,

    /// The task's matrix ID, if any.
    matrix: Option<u64>,

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

    /// We are running the `attempt`-th attempt of the task.
    ///
    /// NOTE: if the task has multiple commands, then restarting will currently go back to the
    /// beginning again, which isn't really what we want ideally. Currently, normal tasks can only
    /// have a single command, so this works out.
    attempt: usize,

    /// The maximum number of failures if `repeat_on_fail` is on.
    maximum_failures: Option<usize>,

    /// The time when this job was enqueued if it has not run yet, or the time when it was started
    /// if it is running or has run.
    timestamp: DateTime<Utc>,

    /// The time when this job entered a Done or Error state (if it has).
    done_timestamp: Option<DateTime<Utc>>,

    /// The length of the timeout if any. The timeout will cause the job to move into the failed
    /// state with a timeout error if it is in the `Running` state for too long.
    timeout: Option<Duration>,

    /// Indicates that a timeout occurred; the usize indicates the index of the cmd that timed
    /// out.
    timedout: Option<usize>,
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
    jids: HashSet<u64>,
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
        let copying_flags = Arc::new(Mutex::new(HashMap::new()));
        let (_copier_thread, copy_thread_queue) = copier::init(Arc::clone(&copying_flags));

        // We keep track of whether any task had a state change. If there was, then there is
        // increased chance of another state change soon, so iterate with higher frequency.
        let mut updated = true;

        loop {
            // Write out state for later (before we grab any locks).
            self.take_snapshot();

            // Sleep 30s, but keep waking up to check if there is likely more work. If there is an
            // indication of potential work, do a full iteration.
            for _ in 0..30000 / 100 {
                std::thread::sleep(std::time::Duration::from_millis(100));

                if updated || self.client_ping.fetch_and(false, Ordering::Relaxed) {
                    break;
                }
            }

            updated = false;

            debug!("Work thread iteration.");

            {
                let mut locked_machines = self.machines.lock().unwrap();
                let mut locked_tasks = self.tasks.lock().unwrap();
                let mut locked_matrices = self.matrices.lock().unwrap();

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
                            copy_thread_queue.clone(),
                        );
                }

                debug!(
                    "Finished driving SMs. Have {} tasks to reap.",
                    to_remove.len()
                );

                // Remove any canceled processes from the task list.
                for jid in to_remove.drain() {
                    let old_task = locked_tasks.remove(&jid);
                    if let Some(Task {
                        jid,
                        matrix: Some(m),
                        ..
                    }) = old_task
                    {
                        let _ = locked_matrices.get_mut(&m).unwrap().jids.remove(&jid);
                    }
                }

                // Remove any empty matrices.
                locked_matrices.retain(|_, matrix| !matrix.jids.is_empty());

                // Clone any failed jobs that need to be cloned.
                for jid in to_clone.drain() {
                    let new_jid = self.next_jid.fetch_add(1, Ordering::Relaxed);
                    // unwrap ok, already checked in `try_drive_sm`
                    let old_task = locked_tasks.get(&jid).unwrap();
                    let task = Self::clone_task(new_jid, old_task);
                    let maybe_matrix = task.matrix.clone();

                    locked_tasks.insert(new_jid, task);

                    if let Some(matrix) = maybe_matrix {
                        locked_matrices
                            .get_mut(&matrix)
                            .unwrap()
                            .jids
                            .insert(new_jid);
                    }
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
                matrix,
                ty: TaskType::Job,
                cmds,
                class,
                variables,
                cp_results,
                timeout,
                state: _,
                machine: _,
                canceled: _,
                repeat_on_fail,
                timestamp: _,
                done_timestamp: _,
                timedout: _,
            } => {
                info!("Cloning job {} to job {}, {:?}", jid, new_jid, task);

                let cmds = cmds.clone();
                let class = class.clone();
                let cp_results = cp_results.clone();
                let variables = variables.clone();

                Task {
                    jid: new_jid,
                    matrix: matrix.clone(),
                    ty: TaskType::Job,
                    cmds,
                    class,
                    cp_results,
                    variables,
                    state: TaskState::Waiting,
                    machine: None,
                    canceled: None,
                    repeat_on_fail: *repeat_on_fail,
                    timestamp: Utc::now(),
                    done_timestamp: None,
                    timeout: *timeout,
                    timedout: None,
                }
            }

            Task {
                jid,
                matrix: None,
                ty: TaskType::SetupTask,
                cmds,
                class,
                machine,
                variables,
                timeout,
                canceled: _,
                state: _,
                cp_results: None,
                repeat_on_fail: false,
                timestamp: _,
                done_timestamp: _,
                timedout: _,
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
                    matrix: None,
                    ty: TaskType::SetupTask,
                    cmds,
                    class,
                    machine,
                    cp_results: None,
                    state: TaskState::Waiting,
                    variables,
                    canceled: None,
                    repeat_on_fail: false,
                    timestamp: Utc::now(),
                    done_timestamp: None,
                    timeout: *timeout,
                    timedout: None,
                }
            }

            _ => unreachable!(),
        }
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
