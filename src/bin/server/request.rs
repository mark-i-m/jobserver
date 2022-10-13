//! Handling of client requests to the server.

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::Ordering;

use chrono::{offset::Utc, Duration};

use expjobserver::{
    cartesian_product, cmd_replace_machine, cmd_replace_vars, cmd_to_path,
    protocol::{
        self,
        request::RequestType::{self, *},
        response::ResponseType::{self, *},
    },
    serialize_ts,
};

use log::{error, info, warn};

use prost::Message;

use super::{MachineStatus, Matrix, Server, Tag, Task, TaskState, TaskType};

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

            TaskState::Running { .. }
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
            TaskState::Running { .. }
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
}

impl Server {
    /// Mark the given job as canceled. This doesn't actually do anything yet. The job will be
    /// killed and removed asynchronously.
    ///
    /// NOTE: this grabs the locks for tasks and live_tasks.
    fn cancel_job(&self, jid: u64, remove: bool) -> ResponseType {
        // We set the `canceled` flag and let the job server handle the rest.

        if let Some(job) = self.tasks.lock().unwrap().get_mut(&jid) {
            info!("Cancelling task {}, {:?}", jid, job);
            job.canceled = Some(remove);
            self.live_tasks.lock().unwrap().insert(jid);
            Okresp(protocol::OkResp {})
        } else {
            error!("No such job: {}", jid);
            Nsjresp(protocol::NoSuchJobResp {})
        }
    }

    pub fn handle_client(&self, mut client: TcpStream) -> std::io::Result<()> {
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
        let response =
            match request {
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
                            failures: 0,
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
                    machine_status: self
                        .machines
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(addr, info)| {
                            (
                                addr.clone(),
                                protocol::MachineStatus {
                                    class: info.class.clone(),
                                    is_free: info.running.is_none(),
                                    running_job: info.running.unwrap_or(0),
                                },
                            )
                        })
                        .collect(),
                }),

                Lvreq(protocol::ListVarsRequest {}) => Vresp(protocol::VarsResp {
                    vars: self.variables.lock().unwrap().clone(),
                }),

                Sumreq(protocol::SetUpMachineRequest {
                    addr,
                    classopt,
                    cmds,
                    timeout,
                }) => {
                    let jid = self.next_jid.fetch_add(1, Ordering::Relaxed);

                    info!(
                        "Create setup task with ID {}. Machine: {}. Cmds: {:?}",
                        jid, addr, cmds
                    );

                    let variables = self.variables.lock().unwrap().clone();
                    let class = classopt
                        .map(|protocol::set_up_machine_request::Classopt::Class(class)| class);

                    self.tasks.lock().unwrap().insert(
                        jid,
                        Task {
                            jid,
                            matrix: None,
                            tag: None,
                            ty: TaskType::SetupTask,
                            cmds,
                            class,
                            machine: Some(addr),
                            state: TaskState::Waiting,
                            variables,
                            cp_results: None,
                            canceled: None,
                            repeat_on_fail: false,
                            maximum_failures: None,
                            attempt: 0,
                            timestamp: Utc::now(),
                            done_timestamp: None,
                            timeout: if timeout > 0 {
                                Some(Duration::minutes(timeout as i64))
                            } else {
                                None
                            },
                            timedout: None,
                            notify: false,
                        },
                    );
                    self.live_tasks.lock().unwrap().insert(jid);

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
                    timeout,
                    maximum_failures,
                    tagidopt: Some(protocol::add_job_request::Tagidopt::Tag(tag)),
                    notify,
                }) => {
                    let jid = self.next_jid.fetch_add(1, Ordering::Relaxed);

                    info!("Added job {} with class {}: {}", jid, class, cmd);

                    let variables = self.variables.lock().unwrap().clone();
                    let cp_results = cp_resultsopt
                        .map(|protocol::add_job_request::CpResultsopt::CpResults(s)| s);

                    let mut locked_tasks = self.tasks.lock().unwrap();
                    let mut locked_live_tasks = self.live_tasks.lock().unwrap();
                    let mut locked_tags = self.tags.lock().unwrap();

                    // We need to check if the tag exists.
                    if let Some(tag) = locked_tags.get_mut(&tag) {
                        locked_tasks.insert(
                            jid,
                            Task {
                                jid,
                                matrix: None,
                                tag: Some(tag.id),
                                ty: TaskType::Job,
                                cmds: vec![cmd],
                                class: Some(class),
                                cp_results,
                                state: TaskState::Waiting,
                                variables,
                                machine: None,
                                canceled: None,
                                repeat_on_fail,
                                maximum_failures: if maximum_failures >= 0 {
                                    Some(maximum_failures as usize)
                                } else {
                                    None
                                },
                                attempt: 0,
                                timestamp: Utc::now(),
                                done_timestamp: None,
                                timeout: if timeout > 0 {
                                    Some(Duration::minutes(timeout as i64))
                                } else {
                                    None
                                },
                                timedout: None,
                                notify,
                            },
                        );
                        tag.jids.insert(jid);
                        locked_live_tasks.insert(jid);

                        Jiresp(protocol::JobIdResp { jid })
                    } else {
                        error!("No such tag: {}", tag);
                        Nstresp(protocol::NoSuchTagResp {})
                    }
                }

                Ajreq(protocol::AddJobRequest {
                    class,
                    cmd,
                    cp_resultsopt,
                    // prost uses a default of `false`.
                    repeat_on_fail,
                    timeout,
                    maximum_failures,
                    tagidopt: None,
                    notify,
                }) => {
                    let jid = self.next_jid.fetch_add(1, Ordering::Relaxed);

                    info!("Added job {} with class {}: {}", jid, class, cmd);

                    let variables = self.variables.lock().unwrap().clone();
                    let cp_results = cp_resultsopt
                        .map(|protocol::add_job_request::CpResultsopt::CpResults(s)| s);

                    // We need to check if the tag exists.
                    self.tasks.lock().unwrap().insert(
                        jid,
                        Task {
                            jid,
                            matrix: None,
                            tag: None,
                            ty: TaskType::Job,
                            cmds: vec![cmd],
                            class: Some(class),
                            cp_results,
                            state: TaskState::Waiting,
                            variables,
                            machine: None,
                            canceled: None,
                            repeat_on_fail,
                            maximum_failures: if maximum_failures >= 0 {
                                Some(maximum_failures as usize)
                            } else {
                                None
                            },
                            attempt: 0,
                            timestamp: Utc::now(),
                            done_timestamp: None,
                            timeout: if timeout > 0 {
                                Some(Duration::minutes(timeout as i64))
                            } else {
                                None
                            },
                            timedout: None,
                            notify,
                        },
                    );
                    self.live_tasks.lock().unwrap().insert(jid);

                    Jiresp(protocol::JobIdResp { jid })
                }

                Ljreq(protocol::ListJobsRequest {}) => {
                    let tasks: Vec<_> = self
                        .tasks
                        .lock()
                        .unwrap()
                        .values()
                        .filter_map(|task| {
                            if task.matrix.is_none() {
                                Some(task.jid)
                            } else {
                                None
                            }
                        })
                        .collect();
                    let running: Vec<_> = self
                        .tasks
                        .lock()
                        .unwrap()
                        .values()
                        .filter_map(|task| {
                            if matches!(task.state, TaskState::Running { .. }) {
                                Some(task.jid)
                            } else {
                                None
                            }
                        })
                        .collect();
                    let matrices: Vec<_> = self
                        .matrices
                        .lock()
                        .unwrap()
                        .values()
                        .map(|matrix| {
                            let cp_resultsopt = matrix.cp_results.as_ref().map(|s| {
                                protocol::matrix_status_resp::CpResultsopt::CpResults(s.into())
                            });
                            protocol::MatrixStatusResp {
                                id: matrix.id,
                                class: matrix.class.clone(),
                                cp_resultsopt,
                                cmd: matrix.cmd.clone(),
                                jobs: matrix.jids.iter().map(|j| *j).collect(),
                                variables: protocol::convert_map(&matrix.variables),
                            }
                        })
                        .collect();
                    let tags = self.tags.lock().unwrap().keys().cloned().collect();

                    Jresp(protocol::JobsResp {
                        jobs: tasks,
                        matrices,
                        running,
                        tags,
                    })
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
                                matrixidopt: task
                                    .unwrap()
                                    .matrix
                                    .map(|m| protocol::job_status_resp::Matrixidopt::Matrix(m)),
                                tagidopt: task
                                    .unwrap()
                                    .tag
                                    .map(|t| protocol::job_status_resp::Tagidopt::Tag(t)),
                                class: class.as_ref().expect("No class for clone").clone(),
                                cmd: cmds.first().unwrap().clone(),
                                status: Some(task.unwrap().status()),
                                variables: variables.clone(),
                                log,
                                timestamp: serialize_ts(task.unwrap().timestamp),
                                donetsop: task.unwrap().done_timestamp.map(serialize_ts).map(
                                    |ts| protocol::job_status_resp::Donetsop::DoneTimestamp(ts),
                                ),
                                cp_results: task
                                    .unwrap()
                                    .cp_results
                                    .as_ref()
                                    .map(Clone::clone)
                                    .unwrap_or_else(|| "".into()),
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
                                TaskState::Running { index } => &cmds[*index],
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
                                let cmd = cmd_replace_machine(
                                    &cmd_replace_vars(&cmd, &variables),
                                    &machine,
                                );
                                format!("{}", cmd_to_path(*jid, &cmd, &self.log_dir))
                            } else {
                                "/dev/null".into()
                            };

                            Jsresp(protocol::JobStatusResp {
                                jid: *jid,
                                matrixidopt: task
                                    .unwrap()
                                    .matrix
                                    .map(|m| protocol::job_status_resp::Matrixidopt::Matrix(m)),
                                tagidopt: task
                                    .unwrap()
                                    .tag
                                    .map(|t| protocol::job_status_resp::Tagidopt::Tag(t)),
                                class: class.as_ref().map(Clone::clone).unwrap_or("".into()),
                                cmd,
                                status: Some(task.unwrap().status()),
                                variables: variables.clone(),
                                log,
                                timestamp: serialize_ts(task.unwrap().timestamp),
                                donetsop: task.unwrap().done_timestamp.map(serialize_ts).map(
                                    |ts| protocol::job_status_resp::Donetsop::DoneTimestamp(ts),
                                ),
                                cp_results: task
                                    .unwrap()
                                    .cp_results
                                    .as_ref()
                                    .map(Clone::clone)
                                    .unwrap_or_else(|| "".into()),
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
                            let maybe_matrix = task.matrix.clone();
                            let maybe_tag = task.tag.clone();

                            locked_jobs.insert(new_jid, task);
                            self.live_tasks.lock().unwrap().insert(new_jid);

                            if let Some(matrix) = maybe_matrix {
                                self.matrices
                                    .lock()
                                    .unwrap()
                                    .get_mut(&matrix)
                                    .unwrap()
                                    .jids
                                    .insert(new_jid);
                            }

                            if let Some(tag) = maybe_tag {
                                self.tags
                                    .lock()
                                    .unwrap()
                                    .get_mut(&tag)
                                    .unwrap()
                                    .jids
                                    .insert(new_jid);
                            }

                            Jiresp(protocol::JobIdResp { jid: new_jid })
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
                    repeat,
                    timeout,
                    maximum_failures,
                }) => {
                    let id = self.next_jid.fetch_add(1, Ordering::Relaxed);

                    let mut vars = protocol::reverse_map(&vars);
                    let cp_results = cp_resultsopt
                        .map(|protocol::add_matrix_request::CpResultsopt::CpResults(s)| s);

                    // Get the set of base variables, some of which may be overridden by the matrix
                    // variables in the template.
                    vars.extend(
                        self.variables
                            .lock()
                            .unwrap()
                            .iter()
                            .map(|(k, v)| (k.to_owned(), vec![v.to_owned()])),
                    );

                    let timeout = if timeout == 0 {
                        None
                    } else {
                        Some(Duration::minutes(timeout as i64))
                    };

                    info!(
                        "Create matrix with ID {}. Cmd: {:?}, Vars: {:?}",
                        id, cmd, vars
                    );

                    let mut jids = HashSet::new();

                    // Create a new job for every element in the cartesian product of the variables.
                    for config in cartesian_product(&vars) {
                        let cmd = cmd_replace_vars(&cmd, &config);

                        for _ in 0..repeat {
                            let jid = self.next_jid.fetch_add(1, Ordering::Relaxed);
                            jids.insert(jid);

                            info!(
                                "[Matrix {}] Added job {} with class {}: {}",
                                id, jid, class, cmd
                            );

                            self.tasks.lock().unwrap().insert(
                                jid,
                                Task {
                                    jid,
                                    matrix: Some(id),
                                    tag: None,
                                    ty: TaskType::Job,
                                    cmds: vec![cmd.clone()],
                                    class: Some(class.clone()),
                                    cp_results: cp_results.clone(),
                                    state: TaskState::Waiting,
                                    variables: config.clone(),
                                    timeout,
                                    machine: None,
                                    canceled: None,
                                    repeat_on_fail: true,
                                    maximum_failures: if maximum_failures >= 0 {
                                        Some(maximum_failures as usize)
                                    } else {
                                        None
                                    },
                                    attempt: 0,
                                    timestamp: Utc::now(),
                                    done_timestamp: None,
                                    timedout: None,
                                    notify: false,
                                },
                            );
                            self.live_tasks.lock().unwrap().insert(jid);
                        }
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

                        let cp_resultsopt = matrix.cp_results.as_ref().map(|s| {
                            protocol::matrix_status_resp::CpResultsopt::CpResults(s.into())
                        });

                        Msresp(protocol::MatrixStatusResp {
                            id,
                            class: matrix.class.clone(),
                            cp_resultsopt,
                            cmd: matrix.cmd.clone(),
                            jobs: matrix.jids.iter().map(|j| *j).collect(),
                            variables: protocol::convert_map(&matrix.variables),
                        })
                    } else {
                        error!("No such matrix: {}", id);
                        Nsmatresp(protocol::NoSuchMatrixResp {})
                    }
                }

                Tjreq(protocol::TagJobRequest { jid, tagopt }) => {
                    match self.tasks.lock().unwrap().get_mut(&jid) {
                        Some(task) => {
                            if let Some(protocol::tag_job_request::Tagopt::Tag(tag)) = tagopt {
                                if let Some(tag) = self.tags.lock().unwrap().get_mut(&tag) {
                                    info!("Added tag {} to job {}", tag.id, jid);
                                    task.tag = Some(tag.id);
                                    tag.jids.insert(jid);

                                    Okresp(protocol::OkResp {})
                                } else {
                                    error!("No such tag: {}", tag);
                                    Nstresp(protocol::NoSuchTagResp {})
                                }
                            } else {
                                if let Some(tag) = task.tag {
                                    info!("Remove tag {} from job {}", tag, jid);

                                    task.tag = None;
                                    self.tags
                                        .lock()
                                        .unwrap()
                                        .get_mut(&tag)
                                        .unwrap()
                                        .jids
                                        .remove(&jid);

                                    Okresp(protocol::OkResp {})
                                } else {
                                    info!("Job {} is already untagged", jid);
                                    Okresp(protocol::OkResp {})
                                }
                            }
                        }

                        None => {
                            error!("No such job or setup task: {}", jid);
                            Nsjresp(protocol::NoSuchJobResp {})
                        }
                    }
                }

                Atreq(protocol::AddTagRequest {}) => {
                    let id = self.next_jid.fetch_add(1, Ordering::Relaxed);
                    self.tags.lock().unwrap().insert(
                        id,
                        Tag {
                            id,
                            jids: HashSet::new(),
                        },
                    );

                    Tidresp(protocol::TagIdResp { id })
                }

                Streq(protocol::StatTagRequest { id }) => {
                    if let Some(tag) = self.tags.lock().unwrap().get(&id) {
                        Tsresp(protocol::TagStatusResp {
                            jobs: tag.jids.iter().cloned().collect(),
                        })
                    } else {
                        error!("No such tag: {}", id);
                        Nstresp(protocol::NoSuchTagResp {})
                    }
                }

                Ajtreq(protocol::AddJobTimeoutRequest { jid, timeout }) => {
                    match self.tasks.lock().unwrap().get_mut(&jid) {
                        Some(task) => {
                            let duration = Duration::minutes(timeout as i64);
                            task.timeout = Some(duration);

                            Okresp(protocol::OkResp {})
                        }

                        None => {
                            error!("No such job or setup task: {}", jid);
                            Nsjresp(protocol::NoSuchJobResp {})
                        }
                    }
                }
            };

        Ok(response)
    }
}
