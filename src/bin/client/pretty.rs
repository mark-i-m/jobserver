//! Utilities for printing nice human-readable output.

use std::collections::{BTreeMap, LinkedList};

use chrono::offset::Utc;

use console::style;

use expjobserver::{human_ts, protocol};

use prettytable::{cell, row, Table};

use super::{JobInfo, JobOrMatrixInfo, MachineInfo, MatrixInfo, Status};

pub(crate) fn print_response(resp: protocol::response::ResponseType) {
    use protocol::{response::ResponseType::*, *};

    match resp {
        Okresp(_) => println!("OK"),
        Mresp(machine_resp) => println!("{:#?}", machine_resp),
        Jresp(jobs_resp) => println!("{:#?}", jobs_resp),
        Vresp(VarsResp { vars }) => println!("{:#?}", vars),
        Jiresp(JobIdResp { jid }) => println!("OK: {}", jid),
        Miresp(MatrixIdResp { id }) => println!("OK: {}", id),
        Jsresp(job_status) => println!("{:#?}", job_status),
        Msresp(matrix_status) => println!("{:#?}", matrix_status),
        Tidresp(TagIdResp { id }) => println!("{:#?}", id),
        Tsresp(tag_status) => println!("{:#?}", tag_status),
        Nsmresp(_) => println!("No such machine."),
        Nsjresp(_) => println!("No such job."),
        Nsmatresp(_) => println!("No such matrix."),
        Nstresp(_) => println!("No such tag."),
        Nwresp(_) => println!("Task is not waiting."),
        Ierr(_) => println!("Internal error."),
    };
}

macro_rules! style {
    ($status:ident, $fmt:literal, $($args:expr),+ $(; $($style:ident),+)?) => {{
        $status += &format!("{}",
            style(format!($fmt, $($args),+))
                $($(. $style () )+)?
        );
    }}
}

struct JobSummary {
    pub total: usize,
    pub running: usize,
    pub failed: usize,
    pub done: usize,
    pub waiting: usize,
    pub held: usize,
    pub canceled: usize,
    pub unknown: usize,
}

impl JobSummary {
    pub fn count<'a>(jobs: impl Iterator<Item = &'a JobInfo>) -> Self {
        let mut summary = JobSummary {
            total: 0,
            running: 0,
            failed: 0,
            done: 0,
            waiting: 0,
            held: 0,
            canceled: 0,
            unknown: 0,
        };

        for job in jobs {
            summary.total += 1;
            match job.status {
                Status::Running { .. } | Status::CopyResults { .. } => summary.running += 1,
                Status::Unknown { .. } => summary.unknown += 1,
                Status::Canceled { .. } => summary.canceled += 1,
                Status::Waiting => summary.waiting += 1,
                Status::Held => summary.held += 1,
                Status::Done { .. } => summary.done += 1,
                Status::Failed { .. } => summary.failed += 1,
            }
        }

        summary
    }
}

/// Compute and print some summary stats.
fn print_summary(items: &[JobOrMatrixInfo]) {
    let stats = JobSummary::count(
        items
            .iter()
            .filter_map(|jomi| {
                if let JobOrMatrixInfo::Job(j) = jomi {
                    Some(j)
                } else {
                    None
                }
            })
            .chain(
                items
                    .iter()
                    .filter_map(|jomi| {
                        if let JobOrMatrixInfo::Matrix(m) = jomi {
                            Some(m)
                        } else {
                            None
                        }
                    })
                    .flat_map(|matrix_info| matrix_info.jobs.iter()),
            ),
    );

    let mut summary = format!("{} jobs: ", stats.total);
    style!(summary, "{} waiting", stats.waiting; blue, bright);
    summary += ", ";
    style!(summary, "{} held", stats.held; blue, bright);
    summary += ", ";
    style!(summary, "{} running", stats.running; yellow);
    summary += ", ";
    style!(summary, "{} done", stats.done; green);
    summary += ", ";
    style!(summary, "{} failed", stats.failed; red, underlined);
    summary += ", ";
    style!(summary, "{} cancelled", stats.canceled; red);
    summary += ", ";
    style!(summary, "{} unknown", stats.unknown; black, bright);

    println!("{}\n", summary);
}

/// Compute the width with which to truncate the cmd in a row, if needed.
const fn row_cmd_width(term_width: u16) -> usize {
    const JID_WIDTH: usize = 15;
    const STATUS_WIDTH: usize = 20;
    const CLASS_WIDTH: usize = 10;
    const MACHINE_WIDTH: usize = 35;
    const OUTPUT_WIDTH: usize = 5;
    const ELLIPSIS_WIDTH: usize = 3;
    const PADDING_WIDTH: usize = 2 * 6;

    const ALTERNATE: usize = usize::MAX;

    const TOTAL: usize = JID_WIDTH
        + STATUS_WIDTH
        + CLASS_WIDTH
        + MACHINE_WIDTH
        + OUTPUT_WIDTH
        + ELLIPSIS_WIDTH
        + PADDING_WIDTH;

    let term_width = term_width as usize;

    // If the the terminal is not wide enough, then we will have wrap-around anyway. Don't
    // bother trying to make everything fit on one line. Instead, try to display everything.
    if term_width > TOTAL {
        term_width - TOTAL
    } else {
        ALTERNATE
    }
}

fn truncate_cmd(cmd: &str, term_width: u16) -> String {
    let cmd_width = row_cmd_width(term_width);

    let mut cmd_trunc = cmd.to_owned();
    cmd_trunc.truncate(cmd_width);
    if cmd_trunc.len() < cmd.len() {
        cmd_trunc.push_str("...");
    }

    cmd_trunc
}

/// Add a row to the table for a task.
fn add_task_row(table: &mut Table, job: JobInfo, term_width: u16) {
    let jid = if let Some(matrix) = job.matrix {
        format!("{}:{}", matrix, job.jid)
    } else if let Some(tag) = job.tag {
        format!("{}:{}", tag, job.jid)
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
            let cmd = truncate_cmd(&cmd, term_width);
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
            let cmd = truncate_cmd(&cmd, term_width);
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
            let cmd = truncate_cmd(&cmd, term_width);
            table.add_row(row![b->jid, FB->status, class, cmd, "", ""]);
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
            let cmd = truncate_cmd(&cmd, term_width);
            table.add_row(row![b->jid, FB->status, class, cmd, "", ""]);
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
            let cmd = truncate_cmd(&cmd, term_width);
            table.add_row(row![b->jid, Fm->status, class, cmd, machine, ""]);
        }

        JobInfo {
            cmd,
            class,
            status:
                Status::Done {
                    machine,
                    output: Some(_),
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
            let cmd = truncate_cmd(&cmd, term_width);
            table.add_row(row![b->jid, Fg->status, class, cmd, machine, Fg->"Ready"]);
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
            let cmd = truncate_cmd(&cmd, term_width);
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
            let cmd = truncate_cmd(&cmd, term_width);
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
            let cmd = truncate_cmd(&cmd, term_width);
            table.add_row(row![b->jid, Fy->status, class, cmd, machine, ""]);
        }
    }
}

/// Add a row to the table representing a whole matrix.
fn add_matrix_row(table: &mut Table, matrix: MatrixInfo, term_width: u16) {
    let stats = JobSummary::count(matrix.jobs.iter());
    let id = format!("{} (matrix)", matrix.id);

    let status = {
        let mut status = String::new();
        if stats.running > 0 {
            style!(status, "{}R", stats.running; yellow);
        }
        if stats.waiting > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}W", stats.waiting; blue, bright);
        }
        if stats.held > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}H", stats.held; blue, bright);
        }
        if stats.done > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}D", stats.done; green);
        }
        if stats.failed > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}F", stats.failed; red, underlined);
        }
        if stats.canceled > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}C", stats.canceled; red);
        }
        if stats.unknown > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}U", stats.unknown; black, bright);
        }

        status
    };

    let cmd = truncate_cmd(&matrix.cmd, term_width);
    table.add_row(row![b->id, status, matrix.class, cmd, "", ""]);
}

fn add_group_row(table: &mut Table, tag: u64, jobs: Vec<JobInfo>) -> () {
    let stats = JobSummary::count(jobs.iter());
    let id = format!("{} (tag)", tag);

    let status = {
        let mut status = String::new();
        if stats.running > 0 {
            style!(status, "{}R", stats.running; yellow);
        }
        if stats.waiting > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}W", stats.waiting; blue, bright);
        }
        if stats.held > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}H", stats.held; blue, bright);
        }
        if stats.done > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}D", stats.done; green);
        }
        if stats.failed > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}F", stats.failed; red, underlined);
        }
        if stats.canceled > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}C", stats.canceled; red);
        }
        if stats.unknown > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}U", stats.unknown; black, bright);
        }

        status
    };

    table.add_row(row![b->id, status, "", "", "", ""]);
}

fn add_line_row(table: &mut Table, _term_width: u16) {
    table.add_empty_row();
    table.add_empty_row();
}

pub(crate) fn print_jobs(
    items: Vec<JobOrMatrixInfo>,
    collapse_matrices: bool,
    collapse_tags: bool,
    line: Option<u64>,
) {
    // Print the summary.
    print_summary(&items);

    // Print a nice human-readable table.
    let term_width = console::Term::stdout().size().1;
    let mut table = Table::new();

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
    table.set_titles(row![ Fwbu =>
        "Job", "Status", "Class", "Command", "Machine", "Output"
    ]);

    let mut tagged = BTreeMap::new();
    let mut untagged = LinkedList::new(); // sorted
    let mut prev_id = None;

    // First, pull out tagged items if needed, so we can collapse them and print the tags in order
    // with other tasks...
    for item in items.into_iter() {
        // Hold onto tagged rows to collapse them, if needed.
        if collapse_tags && item.job().map(|j| j.tag.is_some()).unwrap_or(false) {
            if let JobOrMatrixInfo::Job(job) = item {
                tagged
                    .entry(job.tag.unwrap())
                    .or_insert(Vec::new())
                    .push(job);
            }
        } else {
            untagged.push_back(item);
        }
    }

    // Output in sorted order.
    while !tagged.is_empty() || !untagged.is_empty() {
        // Decide whether to take the next tag or untagged item.
        let take_tag = if tagged.is_empty() {
            false
        } else {
            if untagged.is_empty() {
                true
            } else {
                untagged.front().unwrap().id().jid() > *tagged.iter().next().unwrap().0
            }
        };

        let current_id = if take_tag {
            let (tag, jobs) = {
                let min_tag = *tagged.iter().next().unwrap().0;
                tagged.remove_entry(&min_tag).unwrap()
            };
            add_group_row(&mut table, tag, jobs);

            tag
        } else {
            let item = untagged.pop_front().unwrap();

            let current_id = match &item {
                JobOrMatrixInfo::Job(job_info) => job_info.jid.0,
                JobOrMatrixInfo::Matrix(matrix_info) => matrix_info.id.0,
            };

            // Output the line if necessary.
            if let (Some(line), Some(prev_id)) = (line, prev_id) {
                if prev_id <= line && line < current_id {
                    add_line_row(&mut table, term_width);
                }
            }

            match item {
                JobOrMatrixInfo::Job(job_info) => {
                    add_task_row(&mut table, job_info, term_width);
                }
                JobOrMatrixInfo::Matrix(matrix_info) => {
                    if collapse_matrices {
                        add_matrix_row(&mut table, matrix_info, term_width);
                    } else {
                        for job_info in matrix_info.jobs.into_iter() {
                            add_task_row(&mut table, job_info, term_width);
                        }
                    }
                }
            };

            current_id
        };

        prev_id = Some(current_id);
    }

    table.printstd();
}

pub(crate) fn print_avail(machines: Vec<MachineInfo>) {
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
