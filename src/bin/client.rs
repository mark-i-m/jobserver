//! Client implmentation

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

use clap::clap_app;

use jobserver::{
    cmd_replace_machine, cmd_replace_vars, cmd_to_path, JobServerReq, JobServerResp, Status,
    SERVER_ADDR,
};

use prettytable::{cell, row, Table};

fn main() {
    let matches = clap_app! { client =>
        (about: "CLI client for the jobserver")
        (@arg ADDR: --address +takes_value
         "The server IP:PORT (defaults to `localhost:3030`)")

        (@subcommand ping =>
            (about: "Ping the server")
        )

        (@subcommand mkavail =>
            (about: "Make the given machine available with the given class.")
            (@arg ADDR: +required
             "The IP:PORT of the machine")
            (@arg CLASS: +required
             "The class of the machine")
        )

        (@subcommand rmavail =>
            (about: "Remove the given machine from the available pool.")
            (@arg ADDR: +required
             "The IP:PORT of the machine")
        )

        (@subcommand lsavail =>
            (about: "List available machines.")
        )

        (@subcommand setup =>
            (about: "Set up the given machine using the given command")
            (@arg ADDR: +required
             "The IP:PORT of the machine")
            (@arg CMD: +required ...
             "The setup commands, each as a single string")
            (@arg CLASS: --class +takes_value
             "If passed, the machine is added to the class after setup.")
        )

        (@subcommand lsvars =>
            (about: "List variables and their values.")
        )

        (@subcommand setvar =>
            (about: "Set the given variable to be substituted in commands")
            (@arg NAME: +required
             "The variable name")
            (@arg VALUE: +required
             "The class of the machine")
        )

        (@subcommand addjob =>
            (about: "Add a job to be run on the given class of machine.")
            (@arg CLASS: +required
             "The class of machine that can execute the job")
            (@arg CMD: +required
             "The command to execute")
            (@arg CP_PATH: +required
             "The location on this host to copy results to")
        )

        (@subcommand lsjobs =>
            (about: "List all jobs.")
            (@arg LONG: --long
             "Show all output")
        )

        (@subcommand jobcancel =>
            (about: "Cancel a running/scheduled job OR delete a finished/failed job.")
            (@arg JID: +required ... {is_usize}
             "The job ID(s) of the job(s) to cancel")
        )

        (@subcommand jobstat =>
            (about: "Get information on the status of a job.")
            (@arg JID: +required {is_usize}
             "The job ID of the job")
        )

        (@subcommand jobclone =>
            (about: "Clone a job.")
            (@arg JID: +required {is_usize} ...
             "The job ID(s) of the job to clone.")
        )

        (@subcommand joblog =>
            (about: "Print the path to the job log.")
            (@arg JID: +required {is_usize}
             "The job ID of the job for which to print the log.")
        )

        (@subcommand matrix =>
            (about: "Create a matrix of jobs on the given class of machine.")
            (@arg CLASS: +required
             "The class of machine that can execute the jobs.")
            (@arg CMD: +required
             "The command template to execute with the variables filled in.")
            (@arg CP_PATH: +required
             "The location on this host to copy results to.")
            (@arg VARIABLES: +takes_value +required ...
             "A space-separated list of KEY=VALUE1,VALUE2,... pairs for replacing variables.")
        )

        (@subcommand statmatrix =>
            (about: "Get information on the status of a matrix.")
            (@arg ID: +required {is_usize}
             "The matrix ID of the matrix")
            (@arg LONG: --long
             "Show all output")
        )
    }
    .setting(clap::AppSettings::SubcommandRequired)
    .setting(clap::AppSettings::DisableVersion)
    .get_matches();

    let addr = matches.value_of("ADDR").unwrap_or(SERVER_ADDR);

    match matches.subcommand() {
        ("lsjobs", Some(sub_m)) => {
            let is_long = sub_m.is_present("LONG");

            let jobs = list_jobs(addr);
            print_jobs(jobs, is_long);
        }

        ("lsavail", Some(_sub_m)) => {
            let jobs = list_jobs(addr);
            let avail = list_avail(addr, jobs);
            print_avail(avail);
        }

        ("joblog", Some(sub_m)) => {
            let jid = sub_m.value_of("JID").unwrap();
            get_job_log_path(addr, jid)
        }

        ("jobcancel", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let response = make_request(
                    addr,
                    JobServerReq::CancelJob {
                        jid: jid.parse().unwrap(),
                    },
                );
                println!("Server response: {:?}", response);
            }
        }

        ("jobclone", Some(sub_m)) => {
            for jid in sub_m.values_of("JID").unwrap() {
                let response = make_request(
                    addr,
                    JobServerReq::CloneJob {
                        jid: jid.parse().unwrap(),
                    },
                );
                println!("Server response: {:?}", response);
            }
        }

        ("statmatrix", Some(sub_m)) => {
            let is_long = sub_m.is_present("LONG");

            let response = make_request(
                addr,
                JobServerReq::StatMatrix {
                    id: sub_m.value_of("ID").unwrap().parse().unwrap(),
                },
            );

            match response {
                JobServerResp::MatrixStatus { mut jobs, .. } => {
                    let jobs = stat_jobs(addr, &mut jobs);
                    print_jobs(jobs, is_long);
                }
                _ => println!("Server response: {:?}", response),
            }
        }

        (subcmd, Some(sub_m)) => request_from_subcommand(addr, subcmd, sub_m),

        _ => unreachable!(),
    }
}

fn get_job_log_path(addr: &str, jid: &str) {
    let req = JobServerReq::JobStatus {
        jid: jid.parse().unwrap(),
    };

    let status = make_request(addr, req);

    match status {
        JobServerResp::JobStatus {
            cmd,
            status,
            variables,
            ..
        } => match status {
            Status::Done { machine, .. }
            | Status::Failed {
                machine: Some(machine),
                ..
            }
            | Status::Running { machine } => {
                let cmd = cmd_replace_machine(&cmd_replace_vars(&cmd, &variables), &machine);
                let path = cmd_to_path(&cmd);
                println!("{}", path);
            }

            _ => {
                println!("/dev/null");
            }
        },

        resp => {
            println!("{:?}", resp);
        }
    }
}

fn request_from_subcommand(addr: &str, subcmd: &str, sub_m: &clap::ArgMatches<'_>) {
    // Form the request
    let request = form_request(subcmd, sub_m);

    let response = make_request(addr, request);
    println!("Server response: {:?}", response);
}

fn make_request(server_addr: &str, request: JobServerReq) -> JobServerResp {
    // Connect to server
    let mut tcp_stream = TcpStream::connect(server_addr).expect("Unable to connect to server");

    // Send request
    let request = serde_json::to_string(&request).expect("Unable to serialize message");
    tcp_stream
        .write_all(request.as_bytes())
        .expect("Unable to send message to server");

    // Send EOF
    tcp_stream
        .shutdown(Shutdown::Write)
        .expect("Unable to send EOF to server");

    // Wait for response.
    let mut response = String::new();
    tcp_stream
        .read_to_string(&mut response)
        .expect("Unable to read server response");

    serde_json::from_str(&response).expect("Unable to deserialize server response")
}

fn form_request(subcmd: &str, sub_m: &clap::ArgMatches<'_>) -> JobServerReq {
    match subcmd {
        "ping" => JobServerReq::Ping,

        "mkavail" => JobServerReq::MakeAvailable {
            addr: sub_m.value_of("ADDR").unwrap().into(),
            class: sub_m.value_of("CLASS").unwrap().into(),
        },

        "rmavail" => JobServerReq::RemoveAvailable {
            addr: sub_m.value_of("ADDR").unwrap().into(),
        },

        "lsavail" => JobServerReq::ListAvailable,

        "setup" => JobServerReq::SetUpMachine {
            addr: sub_m.value_of("ADDR").unwrap().into(),
            cmds: sub_m.values_of("CMD").unwrap().map(String::from).collect(),
            class: sub_m.value_of("CLASS").map(Into::into),
        },

        "setvar" => JobServerReq::SetVar {
            name: sub_m.value_of("NAME").unwrap().into(),
            value: sub_m.value_of("VALUE").unwrap().into(),
        },

        "addjob" => JobServerReq::AddJob {
            class: sub_m.value_of("CLASS").unwrap().into(),
            cmd: sub_m.value_of("CMD").unwrap().into(),
            cp_results: sub_m.value_of("CP_PATH").map(Into::into),
        },

        "lsvars" => JobServerReq::ListVars,

        "lsjobs" => JobServerReq::ListJobs,

        "statjob" => JobServerReq::JobStatus {
            jid: sub_m.value_of("JID").unwrap().parse().unwrap(),
        },

        "matrix" => JobServerReq::AddMatrix {
            vars: sub_m
                .values_of("VARIABLES")
                .map(|vals| {
                    vals.map(|val| {
                        let mut spl = val.split("=");
                        let key = spl.next().unwrap().to_string();
                        let value = spl
                            .next()
                            .unwrap()
                            .split(",")
                            .map(|s| s.to_string())
                            .collect();
                        (key, value)
                    })
                    .collect()
                })
                .unwrap_or_else(|| HashMap::new()),
            class: sub_m.value_of("CLASS").unwrap().into(),
            cmd: sub_m.value_of("CMD").unwrap().into(),
            cp_results: sub_m.value_of("CP_PATH").map(Into::into),
        },

        _ => unreachable!(),
    }
}

struct JobInfo {
    class: String,
    cmd: String,
    jid: usize,
    status: Status,
    variables: HashMap<String, String>,
}

fn list_jobs(addr: &str) -> Vec<JobInfo> {
    let job_ids = make_request(addr, JobServerReq::ListJobs);

    if let JobServerResp::Jobs(mut job_ids) = job_ids {
        stat_jobs(addr, &mut job_ids)
    } else {
        unreachable!();
    }
}

fn stat_jobs(addr: &str, jids: &mut Vec<usize>) -> Vec<JobInfo> {
    // Sort by jid
    jids.sort();

    jids.iter()
        .map(|jid| {
            let status = make_request(addr, JobServerReq::JobStatus { jid: *jid });

            if let JobServerResp::JobStatus {
                class,
                cmd,
                jid,
                status,
                variables,
            } = status
            {
                JobInfo {
                    class,
                    cmd,
                    jid,
                    status,
                    variables,
                }
            } else {
                unreachable!();
            }
        })
        .collect()
}

struct MachineInfo {
    addr: String,
    class: String,
    running: Option<usize>,
}

fn list_avail(addr: &str, jobs: Vec<JobInfo>) -> Vec<MachineInfo> {
    let avail = make_request(addr, JobServerReq::ListAvailable);

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

    if let JobServerResp::Machines(machines) = avail {
        let mut avail: Vec<_> = machines
            .into_iter()
            .map(|(machine, class)| {
                let running = running_jobs.remove(&machine);

                MachineInfo {
                    addr: machine,
                    class,
                    running,
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

fn print_jobs(jobs: Vec<JobInfo>, is_long: bool) {
    // Print a nice human-readable table
    let mut table = Table::new();

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);

    table.set_titles(row![ Fwbu =>
        "Job", "Status", "Class", "Command", "Machine", "Output"
    ]);

    const TRUNC: usize = 30;

    // Query each job's status
    for job in jobs.into_iter() {
        match job {
            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Cancelled,
                variables: _variables,
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                table.add_row(row![b->jid, Fri->"Cancelled", class, cmd, "", ""]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Waiting,
                variables: _variables,
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                table.add_row(row![b->jid, Fb->"Waiting", class, cmd, "", ""]);
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
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                table.add_row(row![b->jid, Fm->"Done", class, cmd, machine, ""]);
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
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                let path = if is_long { path } else { "Ready".into() };
                table.add_row(row![b->jid, Fg->"Done", class, cmd, machine, Fg->path]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Failed { error, machine },
                variables: _variables,
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                table.add_row(row![b->jid, Frbu->"Failed", class, cmd,
                              if let Some(machine) = machine { machine } else {"".into()}, error]);
            }

            JobInfo {
                jid,
                mut cmd,
                class,
                status: Status::Running { machine },
                variables: _variables,
            } => {
                if !is_long {
                    cmd.truncate(TRUNC);
                }
                table.add_row(row![b->jid, Fy->"Running", class, cmd, machine, ""]);
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

fn is_usize(s: String) -> Result<(), String> {
    s.as_str()
        .parse::<usize>()
        .map(|_| ())
        .map_err(|e| format!("{:?}", e))
}
