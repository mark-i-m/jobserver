//! Builds the application CLI.

use clap::clap_app;

pub(crate) fn build() -> clap::App<'static, 'static> {
    use clap::{App, AppSettings, Arg, ArgGroup, SubCommand};

    fn is_usize(s: String) -> Result<(), String> {
        s.as_str()
            .parse::<usize>()
            .map(|_| ())
            .map_err(|e| format!("{:?}", e))
    }

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
                .arg(
                    Arg::with_name("ADDR_FILE")
                        .short("f")
                        .long("file")
                        .takes_value(true)
                        .help("A file with one IP:PORT per line"),
                )
                .group(
                    ArgGroup::with_name("MACHINES")
                        .required(true)
                        .multiple(true)
                        .args(&["ADDR", "CLASS", "ADDR_FILE"]),
                ),
        )
        .subcommand(
            SubCommand::with_name("mv")
                .about("Remove the given machine(s) from their current class and add them to a new class.")
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
                .arg(
                    Arg::with_name("ADDR_FILE")
                        .short("f")
                        .long("file")
                        .takes_value(true)
                        .help("A file with one IP:PORT per line"),
                )
                .group(
                    ArgGroup::with_name("MACHINES")
                        .required(true)
                        .multiple(true)
                        .args(&["ADDR", "CLASS", "ADDR_FILE"]),
                )
                .arg(
                    Arg::with_name("NEW_CLASS").takes_value(true).required(true).help("The new class for the machines")
                    )
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
                (@arg JID: {is_usize} ... conflicts_with[N] conflicts_with[RUNNING]
                 "The job IDs of the jobs to list. Unknown job IDs are ignored. \
                  List all jobs if omitted.")
                (@arg N: -n +takes_value {is_usize} conflicts_with[JID] conflicts_with[RUNNING]
                 "List the last N jobs (default: 50)")
                (@arg AFTER: -a --after requires[JID]
                 "List all jobs after the highest given JID.")
                (@arg RUNNING: -r --running conflicts_with[JID] conflicts_with[N]
                 "List all running jobs.")
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
                    (@arg TIMEOUT: --timeout +takes_value {is_usize}
                     "(optional) the timeout for this job in minutes. If the job doesn't \
                      complete within TIMEOUT minutes of entering the \"running\" state, \
                      the job killed.")
                )

                (@subcommand ls =>
                    (about: "Get information on the status of a matrix.")
                    (@setting ArgRequiredElseHelp)
                    (@arg ID: +required {is_usize}
                     "The matrix ID of the matrix")
                )
            )
        )
    }).subcommand(machine_cmds)
}
