//! Common definitions for the client and server.

use std::collections::HashMap;

use chrono::{offset::Utc, DateTime};

use itertools::Itertools;

use log::error;

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/jobserver_proto.rs"));

    impl From<&Vec<String>> for MatrixVarValues {
        fn from(vec: &Vec<String>) -> Self {
            MatrixVarValues {
                values: vec.clone(),
            }
        }
    }

    impl From<&MatrixVarValues> for Vec<String> {
        fn from(mvv: &MatrixVarValues) -> Self {
            mvv.values.clone()
        }
    }

    pub fn convert_map(
        map: &std::collections::HashMap<String, Vec<String>>,
    ) -> std::collections::HashMap<String, MatrixVarValues> {
        map.iter().map(|(k, v)| (k.clone(), v.into())).collect()
    }

    pub fn reverse_map(
        map: &std::collections::HashMap<String, MatrixVarValues>,
    ) -> std::collections::HashMap<String, Vec<String>> {
        map.into_iter()
            .map(|(k, v)| (k.clone(), v.into()))
            .collect()
    }
}

/// The address where the server listens.
pub const SERVER_ADDR: &str = "127.0.0.1:3030";

pub fn cmd_replace_vars(cmd: &str, vars: &HashMap<String, String>) -> String {
    vars.iter().fold(cmd.to_string(), |cmd, (key, value)| {
        cmd.replace(&format!("{{{}}}", key), &value)
    })
}

pub fn cmd_replace_machine(cmd: &str, machine: &str) -> String {
    cmd.replace("{MACHINE}", &machine)
}

pub fn cmd_to_path(jid: u64, cmd: &str, log_dir: &str) -> String {
    let mut name = format!(
        "{}/{}-{}",
        log_dir,
        jid,
        cmd.replace(" ", "_")
            .replace("{", "_")
            .replace("}", "_")
            .replace("/", "_")
    );
    name.truncate(200);
    name
}

// Gets the cartesian product of the given set of variables and their sets of possible values.
pub fn cartesian_product<'v>(
    vars: &'v HashMap<String, Vec<String>>,
) -> impl Iterator<Item = HashMap<String, String>> + 'v {
    vars.iter()
        .map(|(k, vs)| vs.iter().map(move |v| (k.clone(), v.clone())))
        .multi_cartesian_product()
        .map(|config: Vec<(String, String)>| config.into_iter().collect())
}

const TS_FORMAT: &str = "%+"; // ISO 8601 format

pub fn serialize_ts(ts: DateTime<Utc>) -> String {
    ts.format(TS_FORMAT).to_string()
}

pub fn deserialize_ts(s: String) -> DateTime<Utc> {
    DateTime::parse_from_str(&s, TS_FORMAT)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|err| {
            error!("Unable to deserialize timestamp: {:?}, {}", s, err);
            Utc::now()
        })
}
