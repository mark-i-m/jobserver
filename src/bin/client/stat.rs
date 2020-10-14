//! Utilities for implementing the `job stat` subcommand for post-processing of jobs.

use std::collections::HashMap;

use expjobserver::protocol;

use super::{JobInfo, Status};

pub(crate) fn make_matrix_csv(
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
