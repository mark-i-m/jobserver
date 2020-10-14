//! Utilities for implementing the `job stat` subcommand for post-processing of jobs.

use std::collections::BTreeMap;
use std::io::{stdout, Write};
use std::process::{Command, Stdio};

use expjobserver::serialize_ts;

use serde::{Deserialize, Serialize};

use super::{list_jobs, Jid, JobInfo, JobListMode, JobOrMatrixInfo, Status};

/// `JobInfo` but in a purely textual form that can be passed to other processes or formatted for
/// printing to stdout. Missing fields (`None`) are represented as empty strings.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TextJobInfo {
    class: String,
    machine: String,
    cmd: String,
    jid: String,
    matrix: String,
    status: String,
    error: String,
    variables: String,
    results: String,
    start: String,
    end: String,
    log: String,
}

impl Status {
    /// Returns the machine from the given status, if there is one.
    fn machine(&self) -> Option<&str> {
        match self {
            Status::Waiting | Status::Held => None,
            Status::Running { machine }
            | Status::CopyResults { machine }
            | Status::Done { machine, .. } => Some(machine),
            Status::Canceled { machine }
            | Status::Failed { machine, .. }
            | Status::Unknown { machine } => machine.as_ref().map(String::as_str),
        }
    }

    fn to_string(&self) -> String {
        match self {
            Status::Waiting => "Waiting",
            Status::Held => "Held",
            Status::Running { .. } => "Running",
            Status::CopyResults { .. } => "Copy Results",
            Status::Done { .. } => "Done",
            Status::Canceled { .. } => "Cancelled",
            Status::Failed { .. } => "Failed",
            Status::Unknown { .. } => "Unknown",
        }
        .to_owned()
    }
}

impl From<JobInfo> for TextJobInfo {
    fn from(ji: JobInfo) -> Self {
        let status = ji.status.to_string();
        let machine = ji
            .status
            .machine()
            .map(str::to_owned)
            .unwrap_or_else(|| "".into());
        let (results, error) = match ji.status {
            Status::Failed { error, .. } => ("".into(), error),
            Status::Done {
                output: Some(path), ..
            } => (path, "".into()),
            _ => ("".into(), "".into()),
        };

        let variables = serde_json::to_string(&ji.variables).unwrap_or_else(|_| String::new());

        TextJobInfo {
            class: ji.class,
            machine,
            cmd: ji.cmd,
            jid: ji.jid.to_string(),
            matrix: ji
                .matrix
                .map(|m| m.to_string())
                .unwrap_or_else(|| "".into()),
            status,
            error,
            variables,
            results,
            start: serialize_ts(ji.timestamp),
            end: ji
                .done_timestamp
                .map(|t| serialize_ts(t))
                .unwrap_or_else(String::new),
            log: ji.log,
        }
    }
}

pub(crate) fn handle_stat_cmd(addr: &str, sub_m: &clap::ArgMatches<'_>) {
    // Identify and stat all jobs that are to be used.
    let jobs = collect_jobs(addr, sub_m);

    // Pass the relevant columns to the given mappers.
    let jobs = map_jobs(sub_m, jobs);

    // Print the output in the requested format.
    if sub_m.is_present("JSON") {
        print_json(jobs);
    } else if sub_m.is_present("TEXT") {
        print_text(jobs);
    } else if sub_m.is_present("CSV") {
        print_csv(jobs);
    } else {
        unreachable!();
    }
}

fn collect_jobs(addr: &str, sub_m: &clap::ArgMatches<'_>) -> Vec<JobInfo> {
    // Get a combined list of all jobs.
    let after_jids = if let Some(after) = sub_m.value_of("AFTER").map(Jid::from) {
        list_jobs(addr, JobListMode::After(after))
    } else {
        Vec::new()
    };
    let mut running_jids = if sub_m.is_present("RUNNING") {
        list_jobs(addr, JobListMode::Running)
    } else {
        Vec::new()
    };
    let mut listed_jids = sub_m
        .values_of("ID")
        .map(|v| list_jobs(addr, JobListMode::Jids(v.map(Jid::from).collect())))
        .unwrap_or_else(|| Vec::new());

    let mut jids = after_jids;
    jids.append(&mut running_jids);
    jids.append(&mut listed_jids);

    // Flatten the list, so that we only have jobs.
    let mut jobs: Vec<_> = jids
        .into_iter()
        .flat_map(|j| match j {
            JobOrMatrixInfo::Job(ji) => vec![ji],
            JobOrMatrixInfo::Matrix(mi) => mi.jobs,
        })
        .collect();

    // Sort and deduplicate.
    jobs.sort_by_key(|j| j.jid);
    jobs.dedup_by_key(|j| j.jid);

    jobs
}

macro_rules! field_mapper {
    ($job:ident, $argname:literal, $field:ident, $subm:ident) => {{
        if let Some(mapper) = $subm.value_of($argname) {
            let mut child = Command::new(mapper)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("Unable to invoke mapper.");

            {
                let child_stdin = child.stdin.as_mut().unwrap();
                child_stdin
                    .write_all($job.$field.as_bytes())
                    .expect("Unable to write to stdin for mapper.");
            }

            let output = child.wait_with_output().expect("Unable to wait for child.");

            $job.$field = String::from_utf8(output.stdout).expect("Output is not a valid string.");
        }
    }};
}

macro_rules! drop_unselected {
    ($job:ident, $argname:literal, $field:ident, $subm:ident) => {{
        if !$subm.is_present($argname) {
            $job.remove(stringify!($field));
        }
    }};
}

fn map_jobs(sub_m: &clap::ArgMatches<'_>, jobs: Vec<JobInfo>) -> Vec<BTreeMap<String, String>> {
    jobs.into_iter()
        .map(|job| {
            let mut job = TextJobInfo::from(job);

            // Pass the relevant fields to the field mapper commands.
            field_mapper!(job, "PJIDMAP", jid, sub_m);
            field_mapper!(job, "PMIDMAP", matrix, sub_m);
            field_mapper!(job, "PCMDMAP", cmd, sub_m);
            field_mapper!(job, "PCLASSMAP", class, sub_m);
            field_mapper!(job, "PMACHINEMAP", machine, sub_m);
            field_mapper!(job, "PSTATUSMAP", status, sub_m);
            field_mapper!(job, "PERRORMAP", error, sub_m);
            field_mapper!(job, "PVARSMAP", variables, sub_m);
            field_mapper!(job, "PRESULTSMAP", results, sub_m);
            field_mapper!(job, "PSTARTMAP", start, sub_m);
            field_mapper!(job, "PENDMAP", end, sub_m);
            field_mapper!(job, "PLOGMAP", log, sub_m);

            // Pass the whole job to the mapper command.
            let job_json = serde_json::to_string(&job).expect("Unable to serialize to json.");
            let job_json = if let Some(mapper) = sub_m.value_of("MAPPER") {
                let mut child = Command::new(mapper)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()
                    .expect("Unable to invoke mapper.");

                {
                    let child_stdin = child.stdin.as_mut().unwrap();
                    child_stdin
                        .write_all(job_json.as_bytes())
                        .expect("Unable to write to stdin for mapper.");
                }

                let output = child.wait_with_output().expect("Unable to wait for child.");

                String::from_utf8(output.stdout).expect("Output is not a valid string.")
            } else {
                job_json
            };

            // Drop unselected fields.
            let mut job: BTreeMap<_, _> =
                serde_json::from_str(&job_json).expect("Output is not valid json.");

            drop_unselected!(job, "PJID", jid, sub_m);
            drop_unselected!(job, "PMID", matrix, sub_m);
            drop_unselected!(job, "PCMD", cmd, sub_m);
            drop_unselected!(job, "PCLASS", class, sub_m);
            drop_unselected!(job, "PMACHINE", machine, sub_m);
            drop_unselected!(job, "PSTATUS", status, sub_m);
            drop_unselected!(job, "PERROR", error, sub_m);
            drop_unselected!(job, "PVARS", variables, sub_m);
            drop_unselected!(job, "PRESULTS", results, sub_m);
            drop_unselected!(job, "PSTART", start, sub_m);
            drop_unselected!(job, "PEND", end, sub_m);
            drop_unselected!(job, "PLOG", log, sub_m);

            job
        })
        .collect()
}

fn print_json(jobs: Vec<BTreeMap<String, String>>) {
    let json_str = serde_json::to_string(&jobs).expect("Unable to serialize to json");
    println!("{}", json_str);
}

fn print_text(jobs: Vec<BTreeMap<String, String>>) {
    // Print some headers.
    for key in jobs.first().unwrap().keys() {
        print!("{}\t", key);
    }
    println!();

    // Print the data, one job per line.
    for job in jobs.into_iter() {
        for value in job.values() {
            print!("{}\t", value);
        }
        println!();
    }
}

fn print_csv(jobs: Vec<BTreeMap<String, String>>) {
    match print_csv_inner(jobs) {
        Err(e) => panic!("Failed to create CSV: {}", e),
        Ok(()) => {}
    }
}

fn print_csv_inner(jobs: Vec<BTreeMap<String, String>>) -> Result<(), failure::Error> {
    // Simplify our lives by printing nothing if empty.
    if jobs.is_empty() {
        return Ok(());
    }

    // CSV writer to stdout with headers.
    let mut csvw = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(stdout());

    // Write the headers for the CSV.
    let headers = jobs.first().unwrap().keys().collect::<Vec<_>>();
    csvw.write_record(headers)?;

    // Write data with serde.
    for job in jobs.into_iter() {
        csvw.serialize(job)
            .expect("Unable to serialize job to CSV.");
    }

    csvw.flush()?;

    Ok(())
}
