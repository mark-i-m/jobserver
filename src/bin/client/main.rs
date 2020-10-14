//! Client implmentation

use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

#[cfg(target_family = "unix")]
use std::os::unix::process::CommandExt;

use chrono::{offset::Utc, DateTime};

use expjobserver::{
    deserialize_ts,
    protocol::{self, request::RequestType::*, response::ResponseType::*},
    SERVER_ADDR,
};

use prost::Message;

mod cli;
mod pretty;
mod stat;

const DEFAULT_LS_N: usize = 40;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Jid(u64);

impl Jid {
    fn new(jid: u64) -> Self {
        Jid(jid)
    }

    fn jid(self) -> u64 {
        self.0
    }
}

impl<S: AsRef<str>> From<S> for Jid {
    fn from(jid: S) -> Self {
        Jid(jid.as_ref().parse().expect("Unable to parse as usize"))
    }
}

impl From<Jid> for u64 {
    fn from(jid: Jid) -> Self {
        jid.0
    }
}

impl std::fmt::Display for Jid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
/// A more idiomatic version of `protocol::Status`.
pub enum Status {
    /// The job is waiting to run.
    Waiting,

    /// The job is currently running.
    Running {
        /// The machine the job is running on.
        machine: String,
    },

    CopyResults {
        /// The machine the job is running on.
        machine: String,
    },

    /// The job finished runnning.
    Done {
        /// The machine the job is ran on.
        machine: String,

        /// The name of the output files, if any.
        output: Option<String>,
    },

    /// Held.
    Held,

    /// The job was canceled.
    Canceled { machine: Option<String> },

    /// The job produced an error.
    Failed {
        /// The machine where the job was running when the failure occured, if any.
        machine: Option<String>,

        /// The error that caused the failure.
        error: String,
    },

    /// The job is an unknown state (usually due to the server being killed and restarted).
    Unknown { machine: Option<String> },
}

impl From<protocol::Status> for Status {
    fn from(status: protocol::Status) -> Self {
        match status.status() {
            protocol::status::Status::Unknown => Status::Unknown {
                machine: if let Some(protocol::status::Machineopt::Machine(machine)) =
                    status.machineopt
                {
                    Some(machine)
                } else {
                    None
                },
            },

            protocol::status::Status::Waiting => Status::Waiting,

            protocol::status::Status::Running => {
                let protocol::status::Machineopt::Machine(machine) = status.machineopt.unwrap();
                Status::Running { machine }
            }

            protocol::status::Status::Copyresults => {
                let protocol::status::Machineopt::Machine(machine) = status.machineopt.unwrap();
                Status::CopyResults { machine }
            }

            protocol::status::Status::Done => {
                let protocol::status::Machineopt::Machine(machine) = status.machineopt.unwrap();

                Status::Done {
                    machine,
                    output: if let Some(protocol::status::Outputopt::Output(output)) =
                        status.outputopt
                    {
                        Some(output)
                    } else {
                        None
                    },
                }
            }

            protocol::status::Status::Held => Status::Held,

            protocol::status::Status::Canceled => Status::Canceled {
                machine: if let Some(protocol::status::Machineopt::Machine(machine)) =
                    status.machineopt
                {
                    Some(machine)
                } else {
                    None
                },
            },

            protocol::status::Status::Failed => {
                let protocol::status::Erroropt::Error(error) = status.erroropt.unwrap();

                Status::Failed {
                    machine: if let Some(protocol::status::Machineopt::Machine(machine)) =
                        status.machineopt
                    {
                        Some(machine)
                    } else {
                        None
                    },
                    error,
                }
            }
        }
    }
}

fn main() {
    let matches = cli::build().get_matches();
    let addr = matches.value_of("ADDR").unwrap_or(SERVER_ADDR);

    run_inner(addr, &matches)
}

fn run_inner(addr: &str, matches: &clap::ArgMatches<'_>) {
    match matches.subcommand() {
        ("ping", _) => {
            let response = make_request(addr, Preq(protocol::PingRequest {}));
            pretty::print_response(response);
        }

        ("completions", Some(sub_m)) => {
            let bin = sub_m.value_of("BIN");
            let outdir = sub_m.value_of("OUTDIR").unwrap();
            let shell = if sub_m.is_present("bash") {
                clap::Shell::Bash
            } else if sub_m.is_present("fish") {
                clap::Shell::Fish
            } else if sub_m.is_present("zsh") {
                clap::Shell::Zsh
            } else if sub_m.is_present("powershell") {
                clap::Shell::PowerShell
            } else if sub_m.is_present("elvish") {
                clap::Shell::Elvish
            } else {
                unreachable!()
            };
            generate_completions(shell, outdir, bin);
        }

        ("machine", Some(sub_m)) => handle_machine_cmd(addr, sub_m),

        ("var", Some(sub_m)) => handle_var_cmd(addr, sub_m),

        ("job", Some(sub_m)) => handle_job_cmd(addr, sub_m),

        _ => unreachable!(),
    }
}

fn generate_completions(shell: clap::Shell, outdir: &str, bin: Option<&str>) {
    cli::build().gen_completions(bin.unwrap_or("client"), shell, outdir)
}

/// Returns a list of machines from the given command line args.
fn collect_machines_from_cmd(addr: &str, matches: &clap::ArgMatches<'_>) -> Vec<String> {
    let mut addrs = vec![];

    if let Some(args) = matches.values_of("ADDR") {
        addrs.extend(args.map(Into::into));
    }

    if let Some(class) = matches.value_of("CLASS") {
        addrs.extend(list_avail(addr).into_iter().filter_map(|m| {
            if m.class == class {
                Some(m.addr)
            } else {
                None
            }
        }))
    }

    if let Some(addr_file) = matches.value_of("ADDR_FILE") {
        addrs.extend(
            std::fs::read_to_string(addr_file)
                .expect("unable to read address file")
                .split_whitespace()
                .map(|line| line.trim().to_owned()),
        )
    }

    addrs
}

fn handle_machine_cmd(addr: &str, matches: &clap::ArgMatches<'_>) {
    match matches.subcommand() {
        ("ls", Some(_sub_m)) => {
            let avail = list_avail(addr);
            pretty::print_avail(avail);
        }

        ("add", Some(sub_m)) => {
            let req = Mareq(protocol::MakeAvailableRequest {
                addr: sub_m.value_of("ADDR").unwrap().into(),
                class: sub_m.value_of("CLASS").unwrap().into(),
            });

            let response = make_request(addr, req);
            pretty::print_response(response);
        }

        ("mv", Some(sub_m)) => {
            let addrs = collect_machines_from_cmd(addr, sub_m);
            let new_class = sub_m.value_of("NEW_CLASS").unwrap();

            for m in addrs {
                // Remove
                let req = Rareq(protocol::RemoveAvailableRequest { addr: m.to_owned() });
                let response = make_request(addr, req);
                pretty::print_response(response);

                // Add
                let req = Mareq(protocol::MakeAvailableRequest {
                    addr: m.to_owned(),
                    class: new_class.into(),
                });
                let response = make_request(addr, req);
                pretty::print_response(response);
            }
        }

        ("rm", Some(sub_m)) => {
            let addrs = collect_machines_from_cmd(addr, sub_m);

            for m in addrs {
                let req = Rareq(protocol::RemoveAvailableRequest { addr: m.into() });
                let response = make_request(addr, req);
                pretty::print_response(response);
            }
        }

        ("setup", Some(sub_m)) => {
            let machines: Vec<_> = if let Some(addr) = sub_m.values_of("ADDR") {
                addr.map(Into::into).collect()
            } else if let Some(addr_file) = sub_m.value_of("ADDR_FILE") {
                std::fs::read_to_string(addr_file)
                    .expect("unable to read address file")
                    .split_whitespace()
                    .map(|line| line.trim().to_owned())
                    .collect()
            } else if sub_m.is_present("EXISTING") {
                let class = sub_m.value_of("CLASS").unwrap();
                list_avail(addr)
                    .into_iter()
                    .filter_map(|m| if m.class == class { Some(m.addr) } else { None })
                    .collect()
            } else {
                unreachable!();
            };

            let cmds: Vec<_> = sub_m.values_of("CMD").unwrap().map(String::from).collect();
            let class = sub_m
                .value_of("CLASS")
                .map(|s| protocol::set_up_machine_request::Classopt::Class(s.into()));
            let timeout = sub_m
                .value_of("TIMEOUT")
                .map(|s| s.parse().unwrap())
                .unwrap_or(0);

            for machine in machines.into_iter() {
                let req = Sumreq(protocol::SetUpMachineRequest {
                    addr: machine,
                    cmds: cmds.clone(),
                    classopt: class.clone(),
                    timeout,
                });

                let response = make_request(addr, req);
                pretty::print_response(response);
            }
        }

        _ => unreachable!(),
    }
}

fn handle_var_cmd(addr: &str, matches: &clap::ArgMatches<'_>) {
    match matches.subcommand() {
        ("ls", Some(_sub_m)) => {
            let response = make_request(addr, Lvreq(protocol::ListVarsRequest {}));
            pretty::print_response(response);
        }

        ("set", Some(sub_m)) => {
            let req = Svreq(protocol::SetVarRequest {
                name: sub_m.value_of("NAME").unwrap().into(),
                value: sub_m.value_of("VALUE").unwrap().into(),
            });

            let response = make_request(addr, req);
            pretty::print_response(response);
        }

        _ => unreachable!(),
    }
}

fn handle_job_cmd(addr: &str, matches: &clap::ArgMatches<'_>) {
    match matches.subcommand() {
        ("ls", Some(sub_m)) => {
            let suffix = sub_m
                .value_of("N")
                .map(|s| s.parse().unwrap())
                .unwrap_or(DEFAULT_LS_N);
            let is_after = sub_m.is_present("AFTER");
            let is_running = sub_m.is_present("RUNNING");
            let jids = sub_m
                .values_of("JID")
                .map(|v| {
                    if is_after {
                        JobListMode::After(v.map(Jid::from).max().unwrap())
                    } else {
                        JobListMode::Jids(v.map(Jid::from).collect())
                    }
                })
                .unwrap_or_else(|| {
                    if is_running {
                        JobListMode::Running
                    } else {
                        JobListMode::Suffix(suffix)
                    }
                });
            let jobs = list_jobs(addr, jids);
            pretty::print_jobs(jobs, true);
        }

        ("stat", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let req = Jsreq(protocol::JobStatusRequest {
                    jid: Jid::from(jid).into(),
                });

                let response = make_request(addr, req);
                pretty::print_response(response);
            }
        }

        ("hold", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let req = Hjreq(protocol::HoldJobRequest {
                    jid: Jid::from(jid).into(),
                });

                let response = make_request(addr, req);
                pretty::print_response(response);
            }
        }

        ("unhold", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let req = Ujreq(protocol::UnholdJobRequest {
                    jid: Jid::from(jid).into(),
                });

                let response = make_request(addr, req);
                pretty::print_response(response);
            }
        }

        ("log", Some(sub_m)) => {
            let is_err = sub_m.is_present("ERR");
            let jids: Vec<_> = if sub_m.is_present("JID") {
                sub_m.values_of("JID").unwrap().map(Jid::from).collect()
            } else {
                list_jobs(addr, JobListMode::Running)
                    .into_iter()
                    .map(|item| match item {
                        JobOrMatrixInfo::Job(job) => Jid::from(job.jid),
                        _ => unreachable!(),
                    })
                    .collect()
            };

            if jids.is_empty() {
                println!("No jobs selected.");
                return;
            }

            let paths: Vec<_> = jids
                .into_iter()
                .map(|jid| get_job_log_path(addr, jid))
                .map(|path| if is_err { path + ".err" } else { path })
                .collect();

            if sub_m.is_present("LESS") {
                #[cfg(target_family = "unix")]
                {
                    let error = std::process::Command::new("less").args(&paths).exec();
                    println!("exec error: {:?}", error);
                }

                #[cfg(not(target_family = "unix"))]
                {
                    println!("`less` only supported on *nix platforms");
                }
            } else if sub_m.is_present("TAIL") {
                #[cfg(target_family = "unix")]
                {
                    let error = std::process::Command::new("tail")
                        .arg("-f")
                        .args(&paths)
                        .exec();

                    println!("exec error: {:?}", error);
                }

                #[cfg(not(target_family = "unix"))]
                {
                    println!("`tail` only supported on *nix platforms");
                }
            } else {
                println!("{}", paths.join(" "));
            }
        }

        ("add", Some(sub_m)) => {
            let nclones = sub_m
                .value_of("TIMES")
                .map(|s| s.parse().unwrap())
                .unwrap_or(1);
            let retry = sub_m.is_present("RETRY");
            let timeout = sub_m
                .value_of("TIMEOUT")
                .map(|s| s.parse().unwrap())
                .unwrap_or(0);

            for _ in 0..nclones {
                let req = Ajreq(protocol::AddJobRequest {
                    class: sub_m.value_of("CLASS").unwrap().into(),
                    cmd: sub_m.value_of("CMD").unwrap().into(),
                    cp_resultsopt: sub_m
                        .value_of("CP_PATH")
                        .map(|s| protocol::add_job_request::CpResultsopt::CpResults(s.into())),
                    repeat_on_fail: retry,
                    timeout,
                });

                let response = make_request(addr, req);
                pretty::print_response(response);
            }
        }

        ("rm", Some(sub_m)) => {
            let forget = sub_m.is_present("FORGET");
            for jid in sub_m.values_of("JID").unwrap() {
                let response = make_request(
                    addr,
                    Cjreq(protocol::CancelJobRequest {
                        jid: Jid::from(jid).into(),
                        remove: forget,
                    }),
                );
                pretty::print_response(response);
            }
        }

        ("clone", Some(sub_m)) => {
            let nclones = sub_m
                .value_of("TIMES")
                .map(|s| s.parse().unwrap())
                .unwrap_or(1);
            for jid in sub_m.values_of("JID").unwrap() {
                for _ in 0..nclones {
                    let response = make_request(
                        addr,
                        Cljreq(protocol::CloneJobRequest {
                            jid: Jid::from(jid).into(),
                        }),
                    );
                    pretty::print_response(response);
                }
            }
        }

        ("restart", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let response = make_request(
                    addr,
                    Cjreq(protocol::CancelJobRequest {
                        jid: Jid::from(jid).into(),
                        remove: false,
                    }),
                );
                pretty::print_response(response);

                let response = make_request(
                    addr,
                    Cljreq(protocol::CloneJobRequest {
                        jid: Jid::from(jid).into(),
                    }),
                );
                pretty::print_response(response);
            }
        }

        ("matrix", Some(sub_m)) => handle_matrix_cmd(addr, sub_m),

        _ => unreachable!(),
    }
}

fn handle_matrix_cmd(addr: &str, matches: &clap::ArgMatches<'_>) {
    match matches.subcommand() {
        ("add", Some(sub_m)) => {
            let req = Amreq(protocol::AddMatrixRequest {
                vars: sub_m
                    .values_of("VARIABLES")
                    .map(|vals| {
                        vals.map(|val| {
                            let spli = val.find("=").expect("Variables: KEY=VALUE1,VALUE2,...");
                            let (key, values) = val.split_at(spli);
                            let values: Vec<_> =
                                values[1..].split(",").map(|s| s.to_string()).collect();
                            (key.to_owned(), (&values).into())
                        })
                        .collect()
                    })
                    .unwrap_or_else(|| HashMap::new()),
                class: sub_m.value_of("CLASS").unwrap().into(),
                cmd: sub_m.value_of("CMD").unwrap().into(),
                cp_resultsopt: sub_m
                    .value_of("CP_PATH")
                    .map(|s| protocol::add_matrix_request::CpResultsopt::CpResults(s.into())),
                repeat: sub_m
                    .value_of("TIMES")
                    .map(|s| s.parse().unwrap())
                    .unwrap_or(1),
                timeout: sub_m
                    .value_of("TIMEOUT")
                    .map(|s| s.parse().unwrap())
                    .unwrap_or(0),
            });

            let response = make_request(addr, req);
            pretty::print_response(response);
        }

        ("ls", Some(sub_m)) => {
            let response = make_request(
                addr,
                Smreq(protocol::StatMatrixRequest {
                    id: sub_m.value_of("ID").unwrap().parse().unwrap(),
                }),
            );

            match response {
                Msresp(protocol::MatrixStatusResp { jobs, .. }) => {
                    let jobs = jobs.into_iter().map(Jid::new).collect();
                    let jobs = list_jobs(addr, JobListMode::Jids(jobs));
                    pretty::print_jobs(jobs, false);
                }
                _ => pretty::print_response(response),
            }
        }

        _ => unreachable!(),
    }
}

fn get_job_log_path(addr: &str, jid: Jid) -> String {
    let status = make_request(addr, Jsreq(protocol::JobStatusRequest { jid: jid.jid() }));

    match status {
        Jsresp(protocol::JobStatusResp { log, .. }) => log,
        resp => format!("{:#?}", resp),
    }
}

fn make_request(
    server_addr: &str,
    request_ty: protocol::request::RequestType,
) -> protocol::response::ResponseType {
    // Connect to server
    let mut tcp_stream = TcpStream::connect(server_addr).expect("Unable to connect to server");

    // Send request
    let mut request = protocol::Request::default();
    request.request_type = Some(request_ty);
    let mut bytes = vec![];
    request
        .encode(&mut bytes)
        .expect("Unable to serialize message");
    tcp_stream
        .write_all(&bytes)
        .expect("Unable to send message to server");

    // Send EOF
    tcp_stream
        .shutdown(Shutdown::Write)
        .expect("Unable to send EOF to server");

    // Wait for response.
    let mut response = Vec::new();
    tcp_stream
        .read_to_end(&mut response)
        .expect("Unable to read server response");

    protocol::Response::decode(response.as_slice())
        .expect("Unable to deserialize server response")
        .response_type
        .expect("Response is unexpectedly empty.")
}

#[derive(Debug)]
struct JobInfo {
    class: String,
    cmd: String,
    jid: Jid,
    matrix: Option<u64>,
    status: Status,
    variables: HashMap<String, String>,
    cp_results: String,
    timestamp: DateTime<Utc>,
    done_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct MatrixInfoShallow {
    class: String,
    cmd: String,
    id: Jid,
    cp_results: Option<String>,
    jobs: Vec<Jid>,
    variables: HashMap<String, Vec<String>>,
}

#[derive(Debug)]
struct MatrixInfo {
    class: String,
    cmd: String,
    id: Jid,
    cp_results: Option<String>,
    jobs: Vec<JobInfo>,
    variables: HashMap<String, Vec<String>>,
}

#[derive(Debug)]
enum JobListMode {
    /// List a suffix of jobs (the `usize` is the length of the suffix).
    Suffix(usize),

    /// List all jobs starting with the given one.
    After(Jid),

    /// List only JIDs in the set.
    Jids(BTreeSet<Jid>),

    /// List only running jobs.
    Running,
}

#[derive(Debug)]
enum JobOrMatrixInfo {
    Job(JobInfo),
    Matrix(MatrixInfo),
}

impl JobOrMatrixInfo {
    pub fn expect_job(self) -> JobInfo {
        match self {
            JobOrMatrixInfo::Job(job_info) => job_info,
            other => panic!("Expected job, got {:?}", other),
        }
    }
}

fn list_jobs(addr: &str, mode: JobListMode) -> Vec<JobOrMatrixInfo> {
    // Collect info about existing jobs and matrices.
    let job_ids = make_request(addr, Ljreq(protocol::ListJobsRequest {}));
    let (jids, mut matrices, running): (Vec<_>, HashMap<_, _>, Vec<_>) =
        if let Jresp(protocol::JobsResp {
            jobs,
            matrices,
            running,
        }) = job_ids
        {
            let matrices = matrices
                .into_iter()
                .map(
                    |protocol::MatrixStatusResp {
                         cmd,
                         class,
                         id,
                         jobs,
                         variables,
                         cp_resultsopt,
                     }| {
                        (
                            Jid::new(id),
                            MatrixInfoShallow {
                                cmd,
                                class,
                                id: Jid::new(id),
                                jobs: jobs.into_iter().map(Jid::new).collect(),
                                cp_results: cp_resultsopt.map(
                                    |protocol::matrix_status_resp::CpResultsopt::CpResults(s)| s,
                                ),
                                variables: variables
                                    .into_iter()
                                    .map(|(var, protocol::MatrixVarValues { values })| {
                                        (var, values)
                                    })
                                    .collect(),
                            },
                        )
                    },
                )
                .collect();

            let jobs = jobs.into_iter().map(Jid::new).collect();

            let running = running.into_iter().map(Jid::new).collect();

            (jobs, matrices, running)
        } else {
            unreachable!();
        };

    // Collate the jids for which we will dig deeper.
    let selected_ids = {
        let sorted_ids = {
            let mut ids: Vec<_> = jids
                .iter()
                .map(|k| *k)
                .chain(matrices.keys().map(|k| *k))
                .collect();
            ids.sort();
            ids
        };

        let len = sorted_ids.len();
        let mut selected_ids: Vec<_> = sorted_ids
            .into_iter()
            .enumerate()
            .filter(|(i, j)| match mode {
                JobListMode::Suffix(n) => (*i + n >= len) || (len <= n),
                JobListMode::After(jid) => *j >= jid,
                JobListMode::Jids(_) | JobListMode::Running => false,
            })
            .map(|(_, j)| j)
            .collect();

        match mode {
            JobListMode::Jids(ref jids) => {
                selected_ids.extend(jids.iter());
            }
            JobListMode::Running => selected_ids.extend(running.iter()),
            _ => {}
        }

        selected_ids
    };

    let mut info = vec![];
    for id in selected_ids {
        if let Some(MatrixInfoShallow {
            cmd,
            class,
            id,
            jobs,
            cp_results,
            variables,
        }) = matrices.remove(&id)
        {
            let matrix_job_info = jobs.into_iter().filter_map(|jid| stat_job(addr, jid));

            info.push(JobOrMatrixInfo::Matrix(MatrixInfo {
                id,
                cmd,
                class,
                jobs: matrix_job_info.collect(),
                cp_results,
                variables,
            }))
        } else if let Some(job_info) = stat_job(addr, id) {
            info.push(JobOrMatrixInfo::Job(job_info))
        }
    }

    info
}

fn stat_job(addr: &str, jid: Jid) -> Option<JobInfo> {
    let status = make_request(addr, Jsreq(protocol::JobStatusRequest { jid: jid.jid() }));

    if let Jsresp(protocol::JobStatusResp {
        class,
        cmd,
        jid,
        matrixidopt,
        status,
        variables,
        timestamp,
        donetsop,
        log: _,
        cp_results,
    }) = status
    {
        let status = Status::from(status.expect("Status is unexpectedly missing"));
        Some(JobInfo {
            class,
            cmd,
            jid: Jid::new(jid),
            matrix: matrixidopt.map(|protocol::job_status_resp::Matrixidopt::Matrix(id)| id),
            status,
            variables,
            cp_results,
            timestamp: deserialize_ts(timestamp),
            done_timestamp: donetsop
                .map(|protocol::job_status_resp::Donetsop::DoneTimestamp(ts)| deserialize_ts(ts)),
        })
    } else {
        println!("Unable to find job {}", jid);
        None
    }
}

struct MachineInfo {
    addr: String,
    class: String,
    running: Option<Jid>,
}

fn list_avail(addr: &str) -> Vec<MachineInfo> {
    let avail = make_request(addr, Lareq(protocol::ListAvailableRequest {}));

    if let Mresp(protocol::MachinesResp { machine_status }) = avail {
        let mut avail: Vec<_> = machine_status
            .into_iter()
            .map(
                |(
                    machine,
                    protocol::MachineStatus {
                        class,
                        is_free,
                        running_job,
                    },
                )| {
                    MachineInfo {
                        addr: machine,
                        class,
                        running: if is_free {
                            None
                        } else {
                            Some(Jid::new(running_job))
                        },
                    }
                },
            )
            .collect();

        avail.sort_by_key(|m| m.addr.clone());
        avail.sort_by_key(|m| m.class.clone());

        avail
    } else {
        unreachable!();
    }
}
