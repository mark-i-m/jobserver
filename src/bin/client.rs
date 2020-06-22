//! Client implmentation

use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

#[cfg(target_family = "unix")]
use std::os::unix::process::CommandExt;

use chrono::{offset::Utc, DateTime};

use clap::clap_app;

use jobserver::{
    deserialize_ts,
    protocol::{self, request::RequestType::*, response::ResponseType::*},
    SERVER_ADDR,
};

use prettytable::{cell, row, Table};

use prost::Message;

const DEFAULT_LS_N: usize = 40;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
    Canceled,

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
        match status.status {
            0 => Status::Unknown {
                machine: if let Some(protocol::status::Machineopt::Machine(machine)) =
                    status.machineopt
                {
                    Some(machine)
                } else {
                    None
                },
            },

            1 => Status::Waiting,

            2 => {
                let protocol::status::Machineopt::Machine(machine) = status.machineopt.unwrap();
                Status::Running { machine }
            }

            3 => {
                let protocol::status::Machineopt::Machine(machine) = status.machineopt.unwrap();
                Status::CopyResults { machine }
            }

            4 => {
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

            5 => Status::Held,

            6 => Status::Canceled,

            7 => {
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

            _ => Status::Unknown { machine: None },
        }
    }
}

fn build_cli() -> clap::App<'static, 'static> {
    clap_app! { client =>
        (about: "CLI client for the jobserver")
        (@setting SubcommandRequiredElseHelp)
        (@setting DisableVersion)

        (@arg ADDR: --address +takes_value
         "The server IP:PORT (defaults to `localhost:3030`)")

        (@subcommand ping =>
            (about: "Ping the server")
        )

        (@subcommand completions =>
            (about: "Produce shell completion scripts for the client")
            (@setting ArgRequiredElseHelp)
            (@arg OUTDIR: +required
             "The directory to generate the completions script to.")
            (@group SHELL =>
                (@attributes +required)
                (@arg bash: --bash "Generate bash completions")
                (@arg fish: --fish "Generate fish completions")
                (@arg zsh: --zsh "Generate zsh completions")
                (@arg powershell: --powershell "Generate powershell completions")
                (@arg elvish: --elvish "Generate elvish completions")
            )
            (@arg BIN: --bin +takes_value
             "The name of the binary or alias by which the client is invoked. For example, if you \
             alias `j` as the client, you should pass this flag with the value `j`."
            )
        )

        (@subcommand machine =>
            (about: "Operations on the available pool of machines.")
            (@setting SubcommandRequiredElseHelp)

            (@subcommand add =>
                (about: "Make the given machine available with the given class.")
                (@setting ArgRequiredElseHelp)
                (@arg ADDR: +required
                 "The IP:PORT of the machine")
                (@arg CLASS: +required
                 "The class of the machine")
            )

            (@subcommand rm =>
                (about: "Remove the given machine from the available pool.")
                (@setting ArgRequiredElseHelp)
                (@arg ADDR: +required ...
                 "The IP:PORT of the machine")
            )

            (@subcommand ls =>
                (about: "List available machines.")
            )

            (@subcommand setup =>
                (about: "Set up the given machine using the given command")
                (@setting ArgRequiredElseHelp)
                (@group MACHINES =>
                    (@attributes +required)
                    (@arg ADDR: -m --machine +takes_value ...
                     "The IP:PORT of the machine")
                    (@arg ADDR_FILE: -f --file +takes_value
                     "A file with one IP:PORT per line")
                )
                (@arg CMD: +required ...
                 "The setup commands, each as a single string")
                (@arg CLASS: --class +takes_value
                 "If passed, the machine is added to the class after setup.")
            )
        )

        (@subcommand var =>
            (about: "Operations on variables.")
            (@setting SubcommandRequiredElseHelp)

            (@subcommand ls =>
                (about: "List variables and their values.")
            )

            (@subcommand set =>
                (about: "Set the given variable to be substituted in commands")
                (@setting ArgRequiredElseHelp)
                (@arg NAME: +required
                 "The variable name")
                (@arg VALUE: +required
                 "The class of the machine")
            )
        )

        (@subcommand job =>
            (about: "Operations on jobs.")
            (@setting SubcommandRequiredElseHelp)

            (@subcommand add =>
                (about: "Add a job to be run on the given class of machine.")
                (@setting ArgRequiredElseHelp)
                (@arg CLASS: +required
                 "The class of machine that can execute the job")
                (@arg CMD: +required
                 "The command to execute")
                (@arg CP_PATH: +required
                 "The location on this host to copy results to")
                (@arg TIMES: -x --times +takes_value {is_usize}
                 "(optional) the number of copies of this job to submit (default: 1)")
                (@arg RETRY: --retry
                 "(optional) if the job fails, retry until success or cancellation.")
            )

            (@subcommand ls =>
                (about: "List all jobs.")
                (@arg JID: {is_usize} ... conflicts_with[N]
                 "The job IDs of the jobs to list. Unknown job IDs are ignored. \
                  List all jobs if omitted.")
                (@arg LONG: --long conflicts_with[CMD]
                 "Show all output")
                (@arg CMD: --commands conflicts_with[LONG]
                 "Show only job IDs and commands")
                (@arg N: -n +takes_value {is_usize} conflicts_with[JID]
                 "Show the last N jobs (default: 50)")
            )

            (@subcommand rm =>
                (about: "Cancel a running/scheduled job OR delete a finished/failed job.")
                (@setting ArgRequiredElseHelp)
                (@arg JID: +required {is_usize} ...
                 "The job ID(s) of the job(s) to cancel")
                (@arg FORGET: -f --forget
                 "Remove the task from the history and garbage collect it.")
            )

            (@subcommand stat =>
                (about: "Get information on the status of a job.")
                (@setting ArgRequiredElseHelp)
                (@arg JID: +required {is_usize} ...
                 "The job ID of the job")
            )

            (@subcommand hold =>
                (about: "Put the job on hold.")
                (@setting ArgRequiredElseHelp)
                (@arg JID: +required {is_usize} ...
                 "The job ID of the job")
            )

            (@subcommand unhold =>
                (about: "Unold the job.")
                (@setting ArgRequiredElseHelp)
                (@arg JID: +required {is_usize} ...
                 "The job ID of the job")
            )

            (@subcommand clone =>
                (about: "Clone a job.")
                (@setting ArgRequiredElseHelp)
                (@arg JID: +required {is_usize} ...
                 "The job ID(s) of the job to clone.")
                (@arg TIMES: -x --times +takes_value {is_usize}
                 "(optional) the number of clones to make (default: 1)")
            )

            (@subcommand log =>
                (about: "Print the path to the job log.")
                (@setting ArgRequiredElseHelp)
                (@group WHICH =>
                    (@attributes +required)
                    (@arg JID: {is_usize} ...
                     "The job ID of the job for which to print the log path.")
                    (@arg RUNNING: -r --running
                     "Print the log path of all running jobs")
                )
                (@arg ERR: -e --error
                 "Print the error log path.")
                (@arg LESS: -l --less conflicts_with[TAIL]
                 "Pass the log path to `less`")
                (@arg TAIL: -t --tail conflicts_with[LESS]
                 "Pass the log path to `tail -f`")
            )

            (@subcommand matrix =>
                (about: "Operations with job matrices")
                (@setting SubcommandRequiredElseHelp)

                (@subcommand add =>
                    (about: "Create a matrix of jobs on the given class of machine.")
                    (@setting ArgRequiredElseHelp)
                    (@arg CLASS: +required
                     "The class of machine that can execute the jobs.")
                    (@arg CMD: +required
                     "The command template to execute with the variables filled in.")
                    (@arg CP_PATH: +required
                     "The location on this host to copy results to.")
                    (@arg VARIABLES: +takes_value +required ...
                     "A space-separated list of KEY=VALUE1,VALUE2,... pairs for replacing variables.")
                )

                (@subcommand stat =>
                    (about: "Get information on the status of a matrix.")
                    (@setting ArgRequiredElseHelp)
                    (@arg ID: +required {is_usize}
                     "The matrix ID of the matrix")
                    (@group DISPLAY =>
                        (@arg LONG: --long
                         "Show all output")
                        (@arg CMD: --commands
                         "Show only job IDs and commands")
                    )
                )

                (@subcommand csv =>
                    (about: "Output information about the matrix and current/finished jobs \
                     to a CSV file.")
                    (@setting ArgRequiredElseHelp)
                    (@arg ID: +required {is_usize}
                     "The matrix ID of the matrix")
                    (@arg FILE: +required
                     "The name of the file to output to. The file is truncated if it exists.")
                )
            )
        )
    }
}

fn main() {
    let matches = build_cli().get_matches();

    let addr = matches.value_of("ADDR").unwrap_or(SERVER_ADDR);

    run_inner(addr, &matches)
}

fn run_inner(addr: &str, matches: &clap::ArgMatches<'_>) {
    match matches.subcommand() {
        ("ping", _) => {
            let response = make_request(addr, Preq(protocol::PingRequest {}));
            println!("Server response: {:#?}", response);
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
    build_cli().gen_completions(bin.unwrap_or("client"), shell, outdir)
}

fn handle_machine_cmd(addr: &str, matches: &clap::ArgMatches<'_>) {
    match matches.subcommand() {
        ("ls", Some(_sub_m)) => {
            let jobs = list_jobs(addr, JobListMode::All);
            let avail = list_avail(addr, jobs);
            print_avail(avail);
        }

        ("add", Some(sub_m)) => {
            let req = Mareq(protocol::MakeAvailableRequest {
                addr: sub_m.value_of("ADDR").unwrap().into(),
                class: sub_m.value_of("CLASS").unwrap().into(),
            });

            let response = make_request(addr, req);
            println!("Server response: {:#?}", response);
        }

        ("rm", Some(sub_m)) => {
            for m in sub_m.values_of("ADDR").unwrap() {
                let req = Rareq(protocol::RemoveAvailableRequest { addr: m.into() });
                let response = make_request(addr, req);
                println!("Server response: {:#?}", response);
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
            } else {
                unreachable!();
            };

            let cmds: Vec<_> = sub_m.values_of("CMD").unwrap().map(String::from).collect();
            let class = sub_m
                .value_of("CLASS")
                .map(|s| protocol::set_up_machine_request::Classopt::Class(s.into()));

            for machine in machines.into_iter() {
                let req = Sumreq(protocol::SetUpMachineRequest {
                    addr: machine,
                    cmds: cmds.clone(),
                    classopt: class.clone(),
                });

                let response = make_request(addr, req);
                println!("Server response: {:#?}", response);
            }
        }

        _ => unreachable!(),
    }
}

fn handle_var_cmd(addr: &str, matches: &clap::ArgMatches<'_>) {
    match matches.subcommand() {
        ("ls", Some(_sub_m)) => {
            let response = make_request(addr, Lvreq(protocol::ListVarsRequest {}));
            println!("Server response: {:#?}", response);
        }

        ("set", Some(sub_m)) => {
            let req = Svreq(protocol::SetVarRequest {
                name: sub_m.value_of("NAME").unwrap().into(),
                value: sub_m.value_of("VALUE").unwrap().into(),
            });

            let response = make_request(addr, req);
            println!("Server response: {:#?}", response);
        }

        _ => unreachable!(),
    }
}

fn handle_job_cmd(addr: &str, matches: &clap::ArgMatches<'_>) {
    match matches.subcommand() {
        ("ls", Some(sub_m)) => {
            let is_long = sub_m.is_present("LONG");
            let suffix = sub_m
                .value_of("N")
                .map(|s| s.parse().unwrap())
                .unwrap_or(DEFAULT_LS_N);
            let is_cmd = sub_m.is_present("CMD");
            let jids = sub_m
                .values_of("JID")
                .map(|v| JobListMode::Jids(v.map(Jid::from).collect()))
                .unwrap_or(JobListMode::Suffix(suffix));
            let jobs = list_jobs(addr, jids);
            print_jobs(jobs, is_long, is_cmd);
        }

        ("stat", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let req = Jsreq(protocol::JobStatusRequest {
                    jid: Jid::from(jid).into(),
                });

                let response = make_request(addr, req);
                println!("Server response: {:#?}", response);
            }
        }

        ("hold", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let req = Hjreq(protocol::HoldJobRequest {
                    jid: Jid::from(jid).into(),
                });

                let response = make_request(addr, req);
                println!("Server response: {:#?}", response);
            }
        }

        ("unhold", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let req = Ujreq(protocol::UnholdJobRequest {
                    jid: Jid::from(jid).into(),
                });

                let response = make_request(addr, req);
                println!("Server response: {:#?}", response);
            }
        }

        ("log", Some(sub_m)) => {
            let is_err = sub_m.is_present("ERR");
            let jids: Vec<_> = if sub_m.is_present("JID") {
                sub_m.values_of("JID").unwrap().map(Jid::from).collect()
            } else {
                list_jobs(addr, JobListMode::All)
                    .into_iter()
                    .filter_map(|job| {
                        if let Status::Running { .. } = job.status {
                            Some(Jid::from(job.jid))
                        } else {
                            None
                        }
                    })
                    .collect()
            };
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

            for _ in 0..nclones {
                let req = Ajreq(protocol::AddJobRequest {
                    class: sub_m.value_of("CLASS").unwrap().into(),
                    cmd: sub_m.value_of("CMD").unwrap().into(),
                    cp_resultsopt: sub_m
                        .value_of("CP_PATH")
                        .map(|s| protocol::add_job_request::CpResultsopt::CpResults(s.into())),
                    repeat_on_fail: retry,
                });

                let response = make_request(addr, req);
                println!("Server response: {:#?}", response);
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
                println!("Server response: {:#?}", response);
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
                    println!("Server response: {:#?}", response);
                }
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
            });

            let response = make_request(addr, req);
            println!("Server response: {:#?}", response);
        }

        ("stat", Some(sub_m)) => {
            let is_long = sub_m.is_present("LONG");
            let is_cmd = sub_m.is_present("CMD");

            let response = make_request(
                addr,
                Smreq(protocol::StatMatrixRequest {
                    id: sub_m.value_of("ID").unwrap().parse().unwrap(),
                }),
            );

            match response {
                Msresp(protocol::MatrixStatusResp { jobs, .. }) => {
                    let mut jobs = jobs.into_iter().map(Jid::new).collect();
                    let jobs = stat_jobs(addr, &mut jobs);
                    print_jobs(jobs, is_long, is_cmd);
                }
                _ => println!("Server response: {:#?}", response),
            }
        }

        ("csv", Some(sub_m)) => {
            let response = make_request(
                addr,
                Smreq(protocol::StatMatrixRequest {
                    id: sub_m.value_of("ID").unwrap().parse().unwrap(),
                }),
            );

            let file = sub_m.value_of("FILE").unwrap();

            match response {
                Msresp(protocol::MatrixStatusResp {
                    jobs,
                    id,
                    variables,
                    ..
                }) => {
                    let mut jobs = jobs.into_iter().map(Jid::new).collect();
                    let jobs = stat_jobs(addr, &mut jobs);
                    make_matrix_csv(file, id, variables, jobs);
                }
                _ => println!("Server response: {:#?}", response),
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

struct JobInfo {
    class: String,
    cmd: String,
    jid: Jid,
    status: Status,
    variables: HashMap<String, String>,
    timestamp: DateTime<Utc>,
    done_timestamp: Option<DateTime<Utc>>,
}

enum JobListMode {
    /// List all jobs
    All,

    /// List a suffix of jobs (the `usize` is the length of the suffix).
    Suffix(usize),

    /// List only JIDs in the set.
    Jids(BTreeSet<Jid>),
}

fn list_jobs(addr: &str, mode: JobListMode) -> Vec<JobInfo> {
    let job_ids = make_request(addr, Ljreq(protocol::ListJobsRequest {}));

    if let Jresp(protocol::JobsResp { jobs }) = job_ids {
        let len = jobs.len();
        stat_jobs(
            addr,
            &mut jobs
                .into_iter()
                .map(Jid::new)
                .enumerate()
                .filter(|(i, j)| match mode {
                    JobListMode::All => true,
                    JobListMode::Suffix(n) => (*i >= len - n) || (len <= n),
                    JobListMode::Jids(ref jids) => jids.contains(j),
                })
                .map(|(_, j)| j)
                .collect(),
        )
    } else {
        unreachable!();
    }
}

fn stat_jobs(addr: &str, jids: &mut Vec<Jid>) -> Vec<JobInfo> {
    // Sort by jid
    jids.sort();

    jids.iter()
        .filter_map(|jid| {
            let status = make_request(addr, Jsreq(protocol::JobStatusRequest { jid: jid.jid() }));

            if let Jsresp(protocol::JobStatusResp {
                class,
                cmd,
                jid,
                status,
                variables,
                timestamp,
                donetsop,
                log: _,
            }) = status
            {
                let status = Status::from(status.expect("Status is unexpectedly missing"));
                Some(JobInfo {
                    class,
                    cmd,
                    jid: Jid::new(jid),
                    status,
                    variables,
                    timestamp: deserialize_ts(timestamp),
                    done_timestamp: donetsop.map(
                        |protocol::job_status_resp::Donetsop::DoneTimestamp(ts)| deserialize_ts(ts),
                    ),
                })
            } else {
                println!("Unable to find job {}", jid);
                None
            }
        })
        .collect()
}

struct MachineInfo {
    addr: String,
    class: String,
    running: Option<u64>,
}

fn list_avail(addr: &str, jobs: Vec<JobInfo>) -> Vec<MachineInfo> {
    let avail = make_request(addr, Lareq(protocol::ListAvailableRequest {}));

    // Find out which jobs are running
    let mut running_jobs = HashMap::new();
    for job in jobs.into_iter() {
        match job {
            JobInfo {
                jid,
                status: Status::Running { machine },
                ..
            } => {
                let old = running_jobs.insert(machine, jid);
                assert!(old.is_none());
            }

            _ => {}
        }
    }

    if let Mresp(protocol::MachinesResp { machines }) = avail {
        let mut avail: Vec<_> = machines
            .into_iter()
            .map(|(machine, class)| {
                let running = running_jobs.remove(&machine);

                MachineInfo {
                    addr: machine,
                    class,
                    running: running.map(Jid::jid),
                }
            })
            .collect();

        avail.sort_by_key(|m| m.addr.clone());
        avail.sort_by_key(|m| m.class.clone());

        avail
    } else {
        unreachable!();
    }
}

fn print_jobs(jobs: Vec<JobInfo>, is_long: bool, is_cmd: bool) {
    // Print a nice human-readable table
    let mut table = Table::new();

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);

    // If we only want command output...
    if is_cmd {
        table.set_titles(row![ Fwbu => "Job", "Command" ]);
        for JobInfo { jid, cmd, .. } in jobs.into_iter() {
            table.add_row(row![b->jid, cmd]);
        }
        table.printstd();

        return;
    }

    // Full output

    table.set_titles(row![ Fwbu =>
        "Job", "Status", "Class", "Command", "Machine", "Output"
    ]);

    const TRUNC: usize = 50;

    // Query each job's status
    for job in jobs.into_iter() {
        match job {
            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Unknown { machine },
                ..
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                let machine = if let Some(machine) = machine {
                    machine
                } else {
                    "".into()
                };
                table.add_row(row![b->jid, FDi->"Unknown", class, cmd, machine, ""]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Canceled,
                variables: _variables,
                ..
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                table.add_row(row![b->jid, Fri->"Canceled", class, cmd, "", ""]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Waiting,
                variables: _variables,
                timestamp,
                ..
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                let status = format!("Waiting ({})", human_ts(Utc::now() - timestamp));
                table.add_row(row![b->jid, Fb->status, class, cmd, "", ""]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Held,
                variables: _variables,
                timestamp,
                ..
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                let status = format!("Held ({})", human_ts(Utc::now() - timestamp));
                table.add_row(row![b->jid, Fb->status, class, cmd, "", ""]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status:
                    Status::Done {
                        machine,
                        output: None,
                    },
                variables: _variables,
                timestamp,
                done_timestamp,
                ..
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                let status = format!(
                    "Done in {}",
                    human_ts(done_timestamp.unwrap_or_else(|| timestamp) - timestamp)
                );
                table.add_row(row![b->jid, Fm->status, class, cmd, machine, ""]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status:
                    Status::Done {
                        machine,
                        output: Some(path),
                    },
                variables: _variables,
                timestamp,
                done_timestamp,
                ..
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                let path = if is_long { path } else { "Ready".into() };
                let status = format!(
                    "Done in {}",
                    human_ts(done_timestamp.unwrap_or_else(|| timestamp) - timestamp)
                );
                table.add_row(row![b->jid, Fg->status, class, cmd, machine, Fg->path]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Failed { error, machine },
                variables: _variables,
                timestamp,
                done_timestamp,
                ..
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                let status = format!(
                    "Failed in {}",
                    human_ts(done_timestamp.unwrap_or_else(|| timestamp) - timestamp)
                );
                table.add_row(row![b->jid, Frbu->status, class, cmd,
                              if let Some(machine) = machine { machine } else {"".into()}, error]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Running { machine },
                variables: _variables,
                timestamp,
                ..
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                let status = format!("Running ({})", human_ts(Utc::now() - timestamp));
                table.add_row(row![b->jid, Fy->status, class, cmd, machine, ""]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::CopyResults { machine },
                variables: _variables,
                timestamp,
                ..
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                let status = format!("Copy Results ({})", human_ts(Utc::now() - timestamp));
                table.add_row(row![b->jid, Fy->status, class, cmd, machine, ""]);
            }
        }
    }

    table.printstd();
}

fn print_avail(machines: Vec<MachineInfo>) {
    // Print a nice human-readable table
    let mut table = Table::new();

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);

    table.set_titles(row![ Fwbu =>
                     "Machine", "Class", "Running"
    ]);

    // Query each job's status
    for machine in machines.iter() {
        match machine {
            MachineInfo {
                addr,
                class,
                running: Some(running),
            } => {
                table.add_row(row![ Fy =>
                    addr,
                    class,
                        format!("{}", running)
                ]);
            }

            MachineInfo {
                addr,
                class,
                running: None,
            } => {
                table.add_row(row![addr, class, ""]);
            }
        }
    }

    table.printstd();
}

fn make_matrix_csv(
    file: &str,
    id: u64,
    variables: HashMap<String, protocol::MatrixVarValues>,
    jobs: Vec<JobInfo>,
) {
    match make_matrix_csv_inner(file, id, variables, jobs) {
        Err(e) => panic!("Failed to create CSV: {}", e),
        Ok(()) => {}
    }
}

fn make_matrix_csv_inner(
    file: &str,
    id: u64,
    variables: HashMap<String, protocol::MatrixVarValues>,
    jobs: Vec<JobInfo>,
) -> Result<(), failure::Error> {
    let mut csvw = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(file)?;

    // The format of the CSV we want:
    //  matrix id, class, cmd (with replacements), jid, status or results file, var1, var2, var3, ... , varN

    // Collect the variables (in some deterministic order)
    let vars: Vec<_> = variables.keys().map(String::as_str).collect();

    // Write the headers for the CSV
    let mut headers = vec!["Matrix ID", "Class", "Command", "Job ID", "Status"];
    headers.extend(vars.iter());
    csvw.write_record(headers)?;

    // Write the content of the CSV
    for job in jobs.into_iter() {
        let mut record = vec![
            id.to_string(),
            job.class,
            job.cmd,
            job.jid.to_string(),
            match job.status {
                Status::Unknown { .. } => "Unknown".to_owned(),
                Status::Canceled => "Canceled".to_owned(),
                Status::Waiting => "Waiting".to_owned(),
                Status::Held => "Held".to_owned(),
                Status::Running { .. } => "Running".to_owned(),
                Status::CopyResults { .. } => "Copying Results".to_owned(),
                Status::Failed { .. } => "Failed".to_owned(),

                Status::Done { output: None, .. } => "Done (no output)".to_owned(),

                Status::Done {
                    output: Some(path), ..
                } => path,
            },
        ];

        for var in vars.iter() {
            record.push(
                job.variables
                    .get(*var)
                    .cloned()
                    .unwrap_or_else(|| "<missing>".to_owned()),
            );
        }

        csvw.write_record(record)?;
    }

    csvw.flush()?;

    Ok(())
}

fn is_usize(s: String) -> Result<(), String> {
    s.as_str()
        .parse::<usize>()
        .map(|_| ())
        .map_err(|e| format!("{:?}", e))
}

fn human_ts(d: chrono::Duration) -> String {
    let total_seconds = d.num_seconds();

    const SECONDS_PER_MINUTE: i64 = 60;
    const SECONDS_PER_HOUR: i64 = 60 * SECONDS_PER_MINUTE;
    const SECONDS_PER_DAY: i64 = 24 * SECONDS_PER_HOUR;

    let display_days = d.num_days();
    let display_hours = (total_seconds - display_days * SECONDS_PER_DAY) / SECONDS_PER_HOUR;
    let display_minutes =
        (total_seconds - display_days * SECONDS_PER_DAY - display_hours * SECONDS_PER_HOUR)
            / SECONDS_PER_MINUTE;
    let display_seconds = total_seconds
        - display_days * SECONDS_PER_DAY
        - display_hours * SECONDS_PER_HOUR
        - display_minutes * SECONDS_PER_MINUTE;

    let mut display_ts = String::new();

    if display_days > 0 {
        display_ts.push_str(&format!("{}d", display_days));
    }

    if display_hours > 0 {
        display_ts.push_str(&format!("{}h", display_hours));
    }

    if display_minutes > 0 {
        display_ts.push_str(&format!("{}m", display_minutes));
    }

    if display_seconds > 0 {
        display_ts.push_str(&format!("{}s", display_seconds));
    }

    if total_seconds == 0 {
        display_ts.push_str("0s");
    }

    display_ts
}
