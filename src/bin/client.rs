//! Client implmentation

use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

#[cfg(target_family = "unix")]
use std::os::unix::process::CommandExt;

use chrono::{offset::Utc, DateTime};

use clap::clap_app;

use expjobserver::{
    deserialize_ts, human_ts,
    protocol::{self, request::RequestType::*, response::ResponseType::*},
    SERVER_ADDR,
};

use prettytable::{cell, row, Table};

use prost::Message;

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

fn build_cli() -> clap::App<'static, 'static> {
    use clap::{App, AppSettings, Arg, ArgGroup, SubCommand};

    // We build this here because the macro doesn't have shorthand for some of the things we want
    // to do, most notably `multiple` with `number_values(1)`.
    let machine_cmds = App::new("machine")
        .about("Operations on the available pool of machines.")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("add")
                .about("Make the given machine available with the given class.")
                .setting(AppSettings::ArgRequiredElseHelp)
                .arg(
                    Arg::with_name("ADDR")
                        .required(true)
                        .help("The IP:PORT of the machine"),
                )
                .arg(
                    Arg::with_name("CLASS")
                        .required(true)
                        .help("The class of the machine"),
                ),
        )
        .subcommand(SubCommand::with_name("ls").about("List available machines."))
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove the given machine or class of machines from the available pool.")
                .setting(AppSettings::ArgRequiredElseHelp)
                .arg(
                    Arg::with_name("ADDR")
                        .short("m")
                        .long("machine")
                        .takes_value(true)
                        .multiple(true)
                        .number_of_values(1)
                        .help("The IP:PORT of the machine"),
                )
                .arg(
                    Arg::with_name("CLASS")
                        .short("c")
                        .long("class")
                        .takes_value(true)
                        .help("The class of machines to remove"),
                )
                .group(
                    ArgGroup::with_name("MACHINES")
                        .required(true)
                        .multiple(true)
                        .args(&["ADDR", "CLASS"]),
                ),
        )
        .subcommand(
            SubCommand::with_name("setup")
                .about("Set up the given machine using the given command")
                .setting(AppSettings::ArgRequiredElseHelp)
                .arg(
                    Arg::with_name("ADDR")
                        .short("m")
                        .long("machine")
                        .takes_value(true)
                        .multiple(true)
                        .number_of_values(1)
                        .help("The IP:PORT of the machine"),
                )
                .arg(
                    Arg::with_name("ADDR_FILE")
                        .short("f")
                        .long("file")
                        .takes_value(true)
                        .help("A file with one IP:PORT per line"),
                )
                .arg(
                    Arg::with_name("EXISTING")
                        .short("e")
                        .long("existing")
                        .requires("CLASS")
                        .help(
                            "Re-setup machines of the given class, \
                             rather than setting up new machines.",
                        ),
                )
                .group(ArgGroup::with_name("MACHINES").required(true).args(&[
                    "ADDR",
                    "ADDR_FILE",
                    "EXISTING",
                ]))
                .arg(
                    Arg::with_name("CMD")
                        .required(true)
                        .multiple(true)
                        .help("The setup commands, each as a single string"),
                )
                .arg(
                    Arg::with_name("CLASS")
                        .short("c")
                        .long("class")
                        .takes_value(true)
                        .help("If passed, the machine is added to the class after setup."),
                )
                .arg(
                    Arg::with_name("TIMEOUT")
                        .long("timeout")
                        .takes_value(true)
                        .help(
                            "If passed, time out the setup task after TIMEOUT minutes \
                             total for all commands.",
                        ),
                ),
        );

    (clap_app! { client =>
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
                (@arg TIMEOUT: --timeout +takes_value {is_usize}
                 "(optional) the timeout for this job in minutes. If the job doesn't \
                  complete within TIMEOUT minutes of entering the \"running\" state, \
                  the job killed.")
            )

            (@subcommand ls =>
                (about: "List all jobs.")
                (@arg JID: {is_usize} ... conflicts_with[N]
                 "The job IDs of the jobs to list. Unknown job IDs are ignored. \
                  List all jobs if omitted.")
                (@arg N: -n +takes_value {is_usize} conflicts_with[JID]
                 "Show the last N jobs (default: 50)")
                (@arg AFTER: -a --after requires[JID]
                 "List all jobs after the highest given JID.")
                (@arg OUTPUT: --output
                 "Show full output paths.")
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

            (@subcommand restart =>
                (about: "Cancel then clone a job.")
                (@setting ArgRequiredElseHelp)
                (@arg JID: +required {is_usize} ...
                 "The job ID(s) of the job to clone.")
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

            (@subcommand results =>
                (about: "List the results path for each job.")
                (@setting ArgRequiredElseHelp)
                (@arg JID: +required {is_usize} ...
                 "The job ID of the job")
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
                    (@arg TIMES: -x --times +takes_value {is_usize}
                     "(optional) the number of copies of each job to submit (default: 1)")
                )

                (@subcommand stat =>
                    (about: "Get information on the status of a matrix.")
                    (@setting ArgRequiredElseHelp)
                    (@arg ID: +required {is_usize}
                     "The matrix ID of the matrix")
                    (@arg OUTPUT: --output
                     "Show full output paths.")
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
    }).subcommand(machine_cmds)
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
            let avail = list_avail(addr);
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
            if let Some(addrs) = sub_m.values_of("ADDR") {
                for m in addrs {
                    let req = Rareq(protocol::RemoveAvailableRequest { addr: m.into() });
                    let response = make_request(addr, req);
                    println!("Server response: {:#?}", response);
                }
            }

            if let Some(class) = sub_m.value_of("CLASS") {
                let addrs: Vec<String> = list_avail(addr)
                    .into_iter()
                    .filter_map(|m| if m.class == class { Some(m.addr) } else { None })
                    .collect();
                for m in addrs {
                    let req = Rareq(protocol::RemoveAvailableRequest { addr: m.into() });
                    let response = make_request(addr, req);
                    println!("Server response: {:#?}", response);
                }
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
            let suffix = sub_m
                .value_of("N")
                .map(|s| s.parse().unwrap())
                .unwrap_or(DEFAULT_LS_N);
            let is_after = sub_m.is_present("AFTER");
            let show_output = sub_m.is_present("OUTPUT");
            let jids = sub_m
                .values_of("JID")
                .map(|v| {
                    if is_after {
                        JobListMode::After(v.map(Jid::from).max().unwrap())
                    } else {
                        JobListMode::Jids(v.map(Jid::from).collect())
                    }
                })
                .unwrap_or(JobListMode::Suffix(suffix));
            let jobs = list_jobs(addr, jids);
            print_jobs(jobs, show_output, true);
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
                list_jobs(addr, JobListMode::Flat)
                    .into_iter()
                    .map(|item| match item {
                        JobOrMatrixInfo::Job(job) => job,
                        _ => unreachable!(),
                    })
                    .filter_map(|job| {
                        if let Status::Running { .. } = job.status {
                            Some(Jid::from(job.jid))
                        } else {
                            None
                        }
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

        ("restart", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let response = make_request(
                    addr,
                    Cjreq(protocol::CancelJobRequest {
                        jid: Jid::from(jid).into(),
                        remove: false,
                    }),
                );
                println!("Server response: {:#?}", response);

                let response = make_request(
                    addr,
                    Cljreq(protocol::CloneJobRequest {
                        jid: Jid::from(jid).into(),
                    }),
                );
                println!("Server response: {:#?}", response);
            }
        }

        ("results", Some(sub_m)) => {
            let jids = sub_m
                .values_of("JID")
                .unwrap()
                .into_iter()
                .map(|a| Jid::from(a))
                .collect();

            let job_info = list_jobs(addr, JobListMode::Jids(jids));
            let job_info = job_info.into_iter().map(|job| job.expect_job());

            for job in job_info {
                use std::path::PathBuf;

                match job {
                    JobInfo {
                        status:
                            Status::Done {
                                output: Some(output),
                                ..
                            },
                        cp_results,
                        ..
                    } => {
                        let path = PathBuf::from(cp_results)
                            .join(PathBuf::from(output).file_name().unwrap().to_str().unwrap());

                        println!("{}", path.display());
                    }

                    _ => {
                        println!("No output for job {}", job.jid);
                    }
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
                repeat: sub_m
                    .value_of("TIMES")
                    .map(|s| s.parse().unwrap())
                    .unwrap_or(1),
            });

            let response = make_request(addr, req);
            println!("Server response: {:#?}", response);
        }

        ("stat", Some(sub_m)) => {
            let show_output = sub_m.is_present("OUTPUT");

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
                    print_jobs(jobs, show_output, false);
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
                    let jobs = jobs.into_iter().map(Jid::new).collect();
                    let jobs = list_jobs(addr, JobListMode::Jids(jobs));
                    let jobs = jobs.into_iter().map(|job| job.expect_job()).collect();
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
    /// List all jobs in a flat way -- as `JobOrMatrixInfo::Job`. This useful when you just want to
    /// iterate over all tasks.
    Flat,

    /// List a suffix of jobs (the `usize` is the length of the suffix).
    Suffix(usize),

    /// List all jobs starting with the given one.
    After(Jid),

    /// List only JIDs in the set.
    Jids(BTreeSet<Jid>),
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
    let (jids, mut matrices): (Vec<_>, HashMap<_, _>) = if let Jresp(protocol::JobsResp {
        jobs,
        matrices,
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
                            cp_results: cp_resultsopt
                                .map(|protocol::matrix_status_resp::CpResultsopt::CpResults(s)| s),
                            variables: variables
                                .into_iter()
                                .map(|(var, protocol::MatrixVarValues { values })| (var, values))
                                .collect(),
                        },
                    )
                },
            )
            .collect();

        let jobs = jobs.into_iter().map(Jid::new).collect();

        (jobs, matrices)
    } else {
        unreachable!();
    };

    // Collate the infomation into a sorted list, and choose which ones to list further.
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
            JobListMode::Flat => true,
            JobListMode::Suffix(n) => (*i + n >= len) || (len <= n),
            JobListMode::After(jid) => *j >= jid,
            JobListMode::Jids(_) => false,
        })
        .map(|(_, j)| j)
        .collect();

    if let JobListMode::Jids(ref jids) = mode {
        selected_ids.extend(jids.iter());
    }

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

            if matches!(mode, JobListMode::Flat) {
                for job_info in matrix_job_info {
                    info.push(JobOrMatrixInfo::Job(job_info))
                }
            } else {
                info.push(JobOrMatrixInfo::Matrix(MatrixInfo {
                    id,
                    cmd,
                    class,
                    jobs: matrix_job_info.collect(),
                    cp_results,
                    variables,
                }))
            }
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

fn print_jobs(items: Vec<JobOrMatrixInfo>, show_output: bool, collapse_matrices: bool) {
    // First, define some useful stuff...

    // Compute and print some summary stats.
    fn print_summary(items: &[JobOrMatrixInfo]) {
        let mut total_jobs = 0;
        let mut running_jobs = 0;
        let mut failed_jobs = 0;
        let mut done_jobs = 0;
        let mut waiting_jobs = 0;
        let mut canceled_jobs = 0;
        let mut unknown_jobs = 0;

        let mut count_task = |task: &JobInfo| {
            total_jobs += 1;
            match task.status {
                Status::Running { .. } | Status::CopyResults { .. } => running_jobs += 1,
                Status::Unknown { .. } => unknown_jobs += 1,
                Status::Canceled { .. } => canceled_jobs += 1,
                Status::Waiting | Status::Held => waiting_jobs += 1,
                Status::Done { .. } => done_jobs += 1,
                Status::Failed { .. } => failed_jobs += 1,
            }
        };

        for item in items.iter() {
            match item {
                JobOrMatrixInfo::Job(job_info) => count_task(job_info),
                JobOrMatrixInfo::Matrix(matrix_info) => {
                    for job in matrix_info.jobs.iter() {
                        count_task(job);
                    }
                }
            }
        }

        println!(
            "{} jobs: {} waiting, {} running, {} done, {} failed, {} canceled, {} unknown\n",
            total_jobs,
            waiting_jobs,
            running_jobs,
            done_jobs,
            failed_jobs,
            canceled_jobs,
            unknown_jobs
        );
    }

    // Add a row to the table for a task.
    fn add_task_row(table: &mut Table, job: JobInfo, show_output: bool) {
        let jid = if let Some(matrix) = job.matrix {
            format!("{}:{}", matrix, job.jid)
        } else {
            format!("{}", job.jid)
        };

        match job {
            JobInfo {
                cmd,
                class,
                status: Status::Unknown { machine },
                ..
            } => {
                let machine = if let Some(machine) = machine {
                    machine
                } else {
                    "".into()
                };
                table.add_row(row![b->jid, FDi->"Unknown", class, cmd, machine, ""]);
            }

            JobInfo {
                cmd,
                class,
                status: Status::Canceled { machine },
                variables: _variables,
                ..
            } => {
                let machine = if let Some(machine) = machine {
                    machine
                } else {
                    "".into()
                };
                table.add_row(row![b->jid, Fri->"Canceled", class, cmd, machine, ""]);
            }

            JobInfo {
                cmd,
                class,
                status: Status::Waiting,
                variables: _variables,
                timestamp,
                ..
            } => {
                let status = format!("Waiting ({})", human_ts(Utc::now() - timestamp));
                table.add_row(row![b->jid, Fb->status, class, cmd, "", ""]);
            }

            JobInfo {
                cmd,
                class,
                status: Status::Held,
                variables: _variables,
                timestamp,
                ..
            } => {
                let status = format!("Held ({})", human_ts(Utc::now() - timestamp));
                table.add_row(row![b->jid, Fb->status, class, cmd, "", ""]);
            }

            JobInfo {
                cmd,
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
                let status = format!(
                    "Done ({})",
                    human_ts(done_timestamp.unwrap_or_else(|| timestamp) - timestamp)
                );
                table.add_row(row![b->jid, Fm->status, class, cmd, machine, ""]);
            }

            JobInfo {
                cmd,
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
                let path = if show_output { path } else { "Ready".into() };
                let status = format!(
                    "Done ({})",
                    human_ts(done_timestamp.unwrap_or_else(|| timestamp) - timestamp)
                );
                table.add_row(row![b->jid, Fg->status, class, cmd, machine, Fg->path]);
            }

            JobInfo {
                cmd,
                class,
                status: Status::Failed { error, machine },
                variables: _variables,
                timestamp,
                done_timestamp,
                ..
            } => {
                let status = format!(
                    "Failed ({})",
                    human_ts(done_timestamp.unwrap_or_else(|| timestamp) - timestamp)
                );
                table.add_row(row![b->jid, Frbu->status, class, cmd,
                              if let Some(machine) = machine { machine } else {"".into()}, error]);
            }

            JobInfo {
                cmd,
                class,
                status: Status::Running { machine },
                variables: _variables,
                timestamp,
                ..
            } => {
                let status = format!("Running ({})", human_ts(Utc::now() - timestamp));
                table.add_row(row![b->jid, Fy->status, class, cmd, machine, ""]);
            }

            JobInfo {
                cmd,
                class,
                status: Status::CopyResults { machine },
                variables: _variables,
                timestamp,
                ..
            } => {
                let status = format!("Copy Results ({})", human_ts(Utc::now() - timestamp));
                table.add_row(row![b->jid, Fy->status, class, cmd, machine, ""]);
            }
        }
    }

    // Add a row to the table representing a whole matrix.
    fn add_matrix_row(table: &mut Table, matrix: MatrixInfo) {
        let (pending, total) = {
            let mut pending = 0;
            let mut total = 0;
            for j in matrix.jobs.iter() {
                if let Status::CopyResults { .. }
                | Status::Running { .. }
                | Status::Waiting
                | Status::Held = j.status
                {
                    pending += 1;
                }
                total += 1;
            }

            (pending, total)
        };

        let id = format!("{} (matrix)", matrix.id);

        if pending == 0 {
            let status = format!("Done ({} tasks)", total);
            table.add_row(row![b->id, Fg->status, matrix.class, matrix.cmd, "", ""]);
        } else {
            let status = format!("Running ({}/{})", total - pending, total);
            table.add_row(row![b->id, Fy->status, matrix.class, matrix.cmd, "", ""]);
        }
    }

    // Print the summary.
    print_summary(&items);

    // Print a nice human-readable table.
    let mut table = Table::new();

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
    table.set_titles(row![ Fwbu =>
        "Job", "Status", "Class", "Command", "Machine", "Output"
    ]);

    for item in items.into_iter() {
        match item {
            JobOrMatrixInfo::Job(job_info) => add_task_row(&mut table, job_info, show_output),
            JobOrMatrixInfo::Matrix(matrix_info) => {
                if collapse_matrices {
                    add_matrix_row(&mut table, matrix_info);
                } else {
                    for job_info in matrix_info.jobs.into_iter() {
                        add_task_row(&mut table, job_info, show_output);
                    }
                }
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
                Status::Canceled { .. } => "Canceled".to_owned(),
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
