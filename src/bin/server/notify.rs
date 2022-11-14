//! Machinery for sending slack notifications.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use log::{error, info, warn};

use crate::{MachineStatus, Matrix, Server, Task, TaskState};

/// Encapsulated machinery for sending slack notifications.
#[derive(Debug)]
pub struct SlackNotifications {
    /// Enqueued list of events (immediate, summary).
    enqueued_immediate: BTreeSet<Notification>,
    enqueued_summarize: BTreeSet<Notification>,

    /// For each kind of notification, when should we send immediately vs summarize.
    settings: NotificationsSettings,

    /// Time of last summary.
    last_summary: Instant,
}

impl SlackNotifications {
    pub fn new(settings: NotificationsSettings) -> Self {
        SlackNotifications {
            enqueued_immediate: BTreeSet::new(),
            enqueued_summarize: BTreeSet::new(),
            settings,
            last_summary: Instant::now(),
        }
    }
}

/// Whether to summarize or notify immediately a given event.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NotificationSetting {
    Summary,
    Immediate,
}

/// Each of the possible events we could notify on, and how to notify on them.
#[derive(Debug, Clone)]
pub struct NotificationsSettings {
    pub summary_interval: usize, // minutes

    pub running: NotificationSetting,
    pub complete: NotificationSetting,
    pub results_copied: NotificationSetting,
    pub task_failed: NotificationSetting,
    pub machine_broken: NotificationSetting,
}

/// A task or machine for which a notification is needed.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
enum Notification {
    MachineEvent { machine: String },
    TaskEvent { jid: u64 },
}

impl SlackNotifications {
    pub(crate) fn enqueue_task_notification(&mut self, task: &Task) {
        use NotificationSetting::*;

        let t = Notification::TaskEvent { jid: task.jid };

        // If the task isn't requesting notification, then we will only include it in summaries.
        if !task.notify {
            self.enqueued_summarize.insert(t);
            return;
        }

        match &task.state {
            TaskState::Running { .. } => match self.settings.running {
                Immediate => self.enqueued_immediate.insert(t),
                Summary => self.enqueued_summarize.insert(t),
            },

            TaskState::CheckingResults
            | TaskState::Done
            | TaskState::CopyingResults { .. }
            | TaskState::Finalize { results_path: None } => match self.settings.complete {
                Immediate => self.enqueued_immediate.insert(t),
                Summary => self.enqueued_summarize.insert(t),
            },

            TaskState::Finalize {
                results_path: Some(_),
            }
            | TaskState::DoneWithResults { .. } => match self.settings.results_copied {
                Immediate => self.enqueued_immediate.insert(t),
                Summary => self.enqueued_summarize.insert(t),
            },

            TaskState::Error { .. } | TaskState::ErrorDone { .. } => {
                match self.settings.task_failed {
                    Immediate => self.enqueued_immediate.insert(t),
                    Summary => self.enqueued_summarize.insert(t),
                }
            }

            // Don't notify...
            TaskState::Waiting
            | TaskState::Held
            | TaskState::Unknown { .. }
            | TaskState::Killed
            | TaskState::Canceled { .. } => false, // false just to match the other arms' types...
        };

        // We want summaries to include immediately notified events too (it would be weird
        // otherwise), so add them together...
        self.enqueued_summarize
            .extend(self.enqueued_immediate.iter().cloned());
    }

    pub(crate) fn enqueue_machine_notification(&mut self, machine: &str) {
        use NotificationSetting::*;

        let m = Notification::MachineEvent {
            machine: machine.to_owned(),
        };

        match self.settings.machine_broken {
            Immediate => self.enqueued_immediate.insert(m),
            Summary => self.enqueued_summarize.insert(m),
        };
    }
}

impl Server {
    /// Process all enqueued notifications.
    /// NOTE: this grabs a lock!
    pub fn process_notifications(&self) {
        // Careful about lock ordering!
        let locked_variables = self.variables.lock().unwrap();
        let locked_machines = self.machines.lock().unwrap();
        let locked_tasks = self.tasks.lock().unwrap();
        let locked_matrices = self.matrices.lock().unwrap();
        let mut locked_notif = self.notifications.lock().unwrap();

        // If slack variables are not set, then do nothing.
        let Some(slack_api) = locked_variables.get("SLACK_API") else {
            // Empty these so they don't waste memory...
            locked_notif.enqueued_immediate = BTreeSet::new();
            locked_notif.enqueued_summarize = BTreeSet::new();
            warn!("Slack notifications requested but no \
                   SLACK_API endpoint provided.");
            return;
        };
        let slack_user = locked_variables.get("SLACK_USER");

        // Process immediate notifications.
        let mut immediate = BTreeSet::new();
        std::mem::swap(&mut immediate, &mut locked_notif.enqueued_immediate);
        for n in immediate.into_iter() {
            match n {
                Notification::MachineEvent { machine } => {
                    if let Some(ms) = locked_machines.get(&machine) {
                        self.send_individual_machine_notification(
                            &slack_api, slack_user, machine, ms,
                        );
                    }
                }
                Notification::TaskEvent { jid } => {
                    if let Some(task) = locked_tasks.get(&jid) {
                        task.send_individual_notification(&slack_api, slack_user);
                    }
                }
            }
        }

        // The send a summary if enough time has elapsed.
        if Instant::now() - locked_notif.last_summary
            > Duration::from_secs(60 * locked_notif.settings.summary_interval as u64)
        {
            let mut summary = BTreeSet::new();
            std::mem::swap(&mut summary, &mut locked_notif.enqueued_summarize);

            if !summary.is_empty() {
                // Collect lists of all summary elements...
                let mut running_tasks = Vec::new();
                let mut check_res_tasks = Vec::new();
                let mut copy_res_tasks = Vec::new();
                let mut done_no_res_tasks = Vec::new();
                let mut done_res_tasks = Vec::new();

                let mut matrices = Vec::new();

                let mut done_exp_res_tasks = Vec::new();
                let mut error_tasks = Vec::new();
                let mut timeout_tasks = Vec::new();
                let mut broken_machines = Vec::new();

                for n in summary.into_iter() {
                    match n {
                        Notification::MachineEvent { machine } => {
                            broken_machines.push(machine);
                        }
                        Notification::TaskEvent { jid } => {
                            if let Some(task) = locked_tasks.get(&jid) {
                                match &task.state {
                                    TaskState::Running { .. } => {
                                        running_tasks.push(jid);
                                    }
                                    TaskState::CheckingResults => {
                                        check_res_tasks.push(jid);
                                    }
                                    TaskState::CopyingResults { .. } => {
                                        copy_res_tasks.push(jid);
                                    }
                                    TaskState::Done
                                    | TaskState::Finalize { results_path: None }
                                        if matches!(task.cp_results, None) =>
                                    {
                                        done_no_res_tasks.push(jid);
                                    }
                                    TaskState::Done
                                    | TaskState::Finalize { results_path: None } => {
                                        done_exp_res_tasks.push(jid);
                                    }
                                    TaskState::Finalize {
                                        results_path: Some(..),
                                    }
                                    | TaskState::DoneWithResults { .. } => {
                                        done_res_tasks.push(jid);
                                    }

                                    TaskState::Error { .. } | TaskState::ErrorDone { .. }
                                        if task.timedout.is_some() =>
                                    {
                                        timeout_tasks.push(jid);
                                    }
                                    TaskState::Error { .. } | TaskState::ErrorDone { .. } => {
                                        error_tasks.push(jid);
                                    }

                                    _ => {}
                                }

                                if let Some(matrix) = task.matrix {
                                    matrices.push(matrix);
                                }
                            }
                        }
                    }
                }

                let mut msg = String::new();

                if let Some(user) = slack_user {
                    msg.push_str(&format!("<@{user}> "));
                }

                msg.push_str("*Summary*\n\n");

                fn list_jids(out: &mut String, jids: Vec<u64>) {
                    for jid in jids.into_iter() {
                        out.push_str(&format!(" {jid}"));
                    }
                }

                // (done, waiting, running, failed)
                fn matrix_status(
                    locked_tasks: &BTreeMap<u64, Task>,
                    matrix: &Matrix,
                ) -> (usize, usize, usize, usize) {
                    let mut done = 0;
                    let mut waiting = 0;
                    let mut running = 0;
                    let mut failed = 0;

                    for jid in matrix.jids.iter() {
                        let Some(task) = locked_tasks.get(&jid)  else {continue};

                        match &task.state {
                            TaskState::Waiting => {
                                waiting += 1;
                            }

                            TaskState::Running { .. }
                            | TaskState::CheckingResults
                            | TaskState::CopyingResults { .. } => {
                                running += 1;
                            }

                            TaskState::Finalize { .. }
                            | TaskState::Done
                            | TaskState::DoneWithResults { .. } => {
                                done += 1;
                            }

                            TaskState::Error { .. } | TaskState::ErrorDone { .. } => {
                                failed += 1;
                            }

                            _ => {}
                        }
                    }

                    (done, waiting, running, failed)
                }

                if running_tasks.is_empty()
                    && check_res_tasks.is_empty()
                    && copy_res_tasks.is_empty()
                    && done_no_res_tasks.is_empty()
                    && done_exp_res_tasks.is_empty()
                    && matrices.is_empty()
                {
                    msg.push_str(
                        "    • :shrug: No new tasks running, completed, or copying results.\n",
                    );
                }

                if !running_tasks.is_empty() {
                    msg.push_str("    • :running: Started running:");
                    list_jids(&mut msg, running_tasks);
                    msg.push_str("\n")
                }

                if !check_res_tasks.is_empty() {
                    msg.push_str("    • :hourglass_flowing_sand: Completed, checking for results:");
                    list_jids(&mut msg, check_res_tasks);
                    msg.push_str("\n")
                }

                if !copy_res_tasks.is_empty() {
                    msg.push_str("    • :hourglass_flowing_sand: Completed, copying results:");
                    list_jids(&mut msg, copy_res_tasks);
                    msg.push_str("\n")
                }

                if !done_no_res_tasks.is_empty() {
                    msg.push_str("    • :large_green_circle: Completed without results:");
                    list_jids(&mut msg, done_no_res_tasks);
                    msg.push_str("\n")
                }

                if !done_res_tasks.is_empty() {
                    msg.push_str("    • :tada: Completed with results:");
                    list_jids(&mut msg, done_res_tasks);
                    msg.push_str("\n")
                }

                if !matrices.is_empty() {
                    msg.push_str("\n*Matrices*\n");

                    for matrix in matrices {
                        let Some(matrix) = locked_matrices.get(&matrix) else {continue;};
                        let (done, waiting, running, failed) =
                            matrix_status(&*locked_tasks, matrix);
                        msg.push_str(&format!(
                            "    • Matrix {}: {waiting} waiting, \
                         {running} running, {done} done, {failed} failed.\n",
                            matrix.id
                        ));
                    }
                }

                if !done_exp_res_tasks.is_empty()
                    || !timeout_tasks.is_empty()
                    || !error_tasks.is_empty()
                    || !broken_machines.is_empty()
                {
                    msg.push_str("\n*Warnings and Errors*\n");

                    if !done_exp_res_tasks.is_empty() {
                        msg.push_str("    • :warning: Completed without expected results:");
                        list_jids(&mut msg, done_exp_res_tasks);
                        msg.push_str("\n")
                    }

                    if !timeout_tasks.is_empty() {
                        msg.push_str("    • :alarm_clock: Timed out:");
                        list_jids(&mut msg, timeout_tasks);
                        msg.push_str("\n")
                    }

                    if !error_tasks.is_empty() {
                        msg.push_str("    • :x: Failed:");
                        list_jids(&mut msg, error_tasks);
                        msg.push_str("\n")
                    }

                    if !broken_machines.is_empty() {
                        msg.push_str("    • :boom: Machines marked as broken:\n");
                        for machine in broken_machines.into_iter() {
                            msg.push_str(&format!("      • {machine}\n"));
                        }
                    }
                }

                send_slack_notification(slack_api, &msg);
            }

            locked_notif.last_summary = Instant::now();
        }
    }

    fn send_individual_machine_notification(
        &self,
        slack_api: &str,
        slack_user: Option<&String>,
        machine: String,
        ms: &MachineStatus,
    ) {
        let msg = format!(
            ":warning: Machine broken: {machine} ({} failures).",
            ms.failures
        );
        let msg = slack_user
            .map(|u| format!("<@{u}> {msg}"))
            .unwrap_or(msg.to_string());

        send_slack_notification(slack_api, &msg);
    }
}

impl Task {
    pub fn send_individual_notification(&self, slack_api: &str, slack_user: Option<&String>) {
        let (indicator, msg) = match &self.state {
            TaskState::Running { .. } => (
                ":running:",
                format!("running on {}.", self.machine.as_ref().unwrap()),
            ),

            TaskState::CheckingResults => (
                ":hourglass_flowing_sand:",
                "task completed, but need to check for results.".into(),
            ),

            TaskState::CopyingResults { .. } => (
                ":hourglass_flowing_sand:",
                "task completed, copying results.".into(),
            ),

            TaskState::Done | TaskState::Finalize { results_path: None }
                if matches!(self.cp_results, None) =>
            {
                (
                    ":large_green_circle:",
                    "task completed without results.".into(),
                )
            }

            TaskState::Done | TaskState::Finalize { results_path: None } => (
                ":warning:",
                "task completed without results, but results were expected.".into(),
            ),

            TaskState::Finalize {
                results_path: Some(results_path),
            }
            | TaskState::DoneWithResults { results_path } => (
                ":tada:",
                format!("task completed, results copied to {results_path}."),
            ),

            TaskState::Error { error, n } | TaskState::ErrorDone { error, n } => {
                match (self.cmds.len(), self.timedout) {
                    (1, None) => (":x:", format!("error: {error}")),
                    (_, None) => (":x:", format!("error on command {n}: {error}")),
                    (1, Some(_)) => (":alarm_clock: :boom:", format!("timed out")),
                    (_, Some(_)) => (":alarm_clock: :boom:", format!("command {n} timed out")),
                }
            }

            // Don't notify...
            TaskState::Waiting
            | TaskState::Held
            | TaskState::Unknown { .. }
            | TaskState::Killed
            | TaskState::Canceled { .. } => unreachable!(),
        };

        let msg = slack_user
            .map(|u| format!("<@{u}> {indicator} Task {}: {msg}", self.jid))
            .unwrap_or(msg.to_string());

        send_slack_notification(slack_api, &msg);
    }
}

fn send_slack_notification(slack_api: &str, msg: &str) {
    let client = reqwest::blocking::Client::new();
    let mut params = BTreeMap::new();
    let msg =
        format!(r#"[ {{ "type": "section", "text": {{ "type": "mrkdwn", "text": "{msg}" }} }} ]"#);
    params.insert("blocks", &msg);
    let req = client.post(slack_api).json(&params);
    info!("Notify: {req:?}");
    match req.send() {
        Ok(resp) if resp.status().is_success() => {
            info!("Notified Slack.")
        }
        Ok(resp) => {
            error!("Failed to send message {msg:?} to Slack: {resp:?}");
        }
        Err(err) => {
            error!("Failed to send message {msg:?} to Slack: {err}");
        }
    }
}
