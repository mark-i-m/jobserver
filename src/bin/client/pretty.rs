//! Utilities for printing nice human-readable output.

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
        Nsmresp(_) => println!("No such machine."),
        Nsjresp(_) => println!("No such job."),
        Nsmatresp(_) => println!("No such matrix."),
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

/// Compute and print some summary stats.
fn print_summary(items: &[JobOrMatrixInfo]) {
    let mut total_jobs = 0;
    let mut running_jobs = 0;
    let mut failed_jobs = 0;
    let mut done_jobs = 0;
    let mut waiting_jobs = 0;
    let mut held_jobs = 0;
    let mut canceled_jobs = 0;
    let mut unknown_jobs = 0;

    let mut count_task = |task: &JobInfo| {
        total_jobs += 1;
        match task.status {
            Status::Running { .. } | Status::CopyResults { .. } => running_jobs += 1,
            Status::Unknown { .. } => unknown_jobs += 1,
            Status::Canceled { .. } => canceled_jobs += 1,
            Status::Waiting => waiting_jobs += 1,
            Status::Held => held_jobs += 1,
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

    let mut summary = format!("{} jobs: ", total_jobs);
    style!(summary, "{} waiting", waiting_jobs; blue, bright);
    summary += ", ";
    style!(summary, "{} held", held_jobs; blue, bright);
    summary += ", ";
    style!(summary, "{} running", running_jobs; yellow);
    summary += ", ";
    style!(summary, "{} done", done_jobs; green);
    summary += ", ";
    style!(summary, "{} failed", failed_jobs; red, underlined);
    summary += ", ";
    style!(summary, "{} cancelled", canceled_jobs; red);
    summary += ", ";
    style!(summary, "{} unknown", unknown_jobs; black, bright);

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
    let (running, waiting, held, done, failed, cancelled, unknown) = {
        let mut running = 0;
        let mut waiting = 0;
        let mut held = 0;
        let mut done = 0;
        let mut failed = 0;
        let mut cancelled = 0;
        let mut unknown = 0;
        for j in matrix.jobs.iter() {
            match j.status {
                Status::Running { .. } | Status::CopyResults { .. } => running += 1,
                Status::Waiting => waiting += 1,
                Status::Held => held += 1,
                Status::Done { .. } => done += 1,
                Status::Failed { .. } => failed += 1,
                Status::Canceled { .. } => cancelled += 1,
                Status::Unknown { .. } => unknown += 1,
            }
        }

        (running, waiting, held, done, failed, cancelled, unknown)
    };

    let id = format!("{} (matrix)", matrix.id);

    let status = {
        let mut status = String::new();
        if running > 0 {
            style!(status, "{}R", running; yellow);
        }
        if waiting > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}W", waiting; blue, bright);
        }
        if held > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}H", held; blue, bright);
        }
        if done > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}D", done; green);
        }
        if failed > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}F", failed; red, underlined);
        }
        if cancelled > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}C", cancelled; red);
        }
        if unknown > 0 {
            if !status.is_empty() {
                status.push_str(" ");
            }
            style!(status, "{}U", unknown; black, bright);
        }

        status
    };

    let cmd = truncate_cmd(&matrix.cmd, term_width);
    table.add_row(row![b->id, status, matrix.class, cmd, "", ""]);
}

pub(crate) fn print_jobs(items: Vec<JobOrMatrixInfo>, collapse_matrices: bool) {
    // Print the summary.
    print_summary(&items);

    // Print a nice human-readable table.
    let term_width = console::Term::stdout().size().1;
    let mut table = Table::new();

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
    table.set_titles(row![ Fwbu =>
        "Job", "Status", "Class", "Command", "Machine", "Output"
    ]);

    for item in items.into_iter() {
        match item {
            JobOrMatrixInfo::Job(job_info) => add_task_row(&mut table, job_info, term_width),
            JobOrMatrixInfo::Matrix(matrix_info) => {
                if collapse_matrices {
                    add_matrix_row(&mut table, matrix_info, term_width);
                } else {
                    for job_info in matrix_info.jobs.into_iter() {
                        add_task_row(&mut table, job_info, term_width);
                    }
                }
            }
        }
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
