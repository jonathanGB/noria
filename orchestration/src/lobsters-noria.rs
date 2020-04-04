#![feature(try_blocks)]

use clap::{value_t, App, Arg};
use rusoto_core::Region;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::io::{self, prelude::*};
use std::{thread, time};
use tsunami::*;
use yansi::Paint;

const AMI: &str = "ami-089ff13f1c92f991e";

fn git_and_cargo(
    ssh: &mut Session,
    dir: &str,
    package: Option<&str>,
    bin: &str,
    branch: Option<&str>,
    on: &str,
) -> Result<(), failure::Error> {
    ssh.exec_print_nonempty(&["git", "-C", dir, "reset", "--hard", "2>&1"], on)?;

    if let Some(branch) = branch {
        ssh.exec_print_nonempty(&["git", "-C", dir, "checkout", branch, "2>&1"], on)?;
    }

    ssh.exec_print_nonempty(&["git", "-C", dir, "pull", "2>&1"], on)?;

    if !bin.is_empty() {
        if let Some(p) = package {
            ssh.exec_print_nonempty(
                &[
                    "cd",
                    dir,
                    "&&",
                    "cargo",
                    "b",
                    "--release",
                    "-p",
                    p,
                    "--bin",
                    bin,
                    "2>&1",
                ],
                on,
            )?;
        } else {
            ssh.exec_print_nonempty(
                &[
                    "cd",
                    dir,
                    "&&",
                    "cargo",
                    "b",
                    "--release",
                    "--bin",
                    bin,
                    "2>&1",
                ],
                on,
            )?;
        }
    }

    Ok(())
}

fn main() {
    let args = App::new("noria lobsters ec2 orchestrator")
        .about("Run the noria lobste.rs benchmark on ec2")
        .arg(
            Arg::with_name("memory_limit")
                .takes_value(true)
                .long("memory-limit")
                .help("Partial state size limit / eviction threshold [in bytes]."),
        )
        .arg(
            Arg::with_name("availability_zone")
                .long("availability-zone")
                .value_name("AZ")
                .default_value("us-east-1a")
                .takes_value(true)
                .help("EC2 availability zone to use for launching instances"),
        )
        .arg(
            Arg::with_name("in-flight")
                .takes_value(true)
                .long("in-flight")
                .default_value("256")
                .help("How many in-flight requests to allow"),
        )
        .arg(
            Arg::with_name("branch")
                .takes_value(true)
                .long("branch")
                .default_value("master")
                .help("Which branch of noria to benchmark"),
        )
        .arg(
            Arg::with_name("SCALE")
                .help("Run the given scale(s).")
                .multiple(true),
        )
        .get_matches();

    let az = args.value_of("availability_zone").unwrap();

    let mut b = TsunamiBuilder::default();
    b.set_region(Region::UsEast1);
    b.set_availability_zone(az);
    b.use_term_logger();
    let branch = args.value_of("branch").map(String::from);
    b.add_set(
        "trawler",
        1,
        MachineSetup::new("m5n.24xlarge", AMI, move |ssh| {
            git_and_cargo(
                ssh,
                "noria",
                Some("noria-applications"),
                "lobsters-noria",
                branch.as_ref().map(String::as_str),
                "client",
            )?;
            Ok(())
        })
        .as_user("ubuntu"),
    );
    let branch = args.value_of("branch").map(String::from);
    let in_flight = clap::value_t!(args, "in-flight", usize).unwrap_or_else(|e| e.exit());
    b.add_set(
        "server",
        1,
        MachineSetup::new("r5n.4xlarge", AMI, move |ssh| {
            git_and_cargo(
                ssh,
                "noria",
                None,
                "noria-server",
                branch.as_ref().map(String::as_str),
                "server",
            )?;
            git_and_cargo(
                ssh,
                "noria",
                None,
                "noria-zk",
                branch.as_ref().map(String::as_str),
                "server",
            )?;
            // we'll need zookeeper running
            ssh.cmd("sudo systemctl start zookeeper")?;

            Ok(())
        })
        .as_user("ubuntu"),
    );

    // https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
    //let sts = rusoto_sts::StsClient::new(rusoto_core::Region::EuCentral1);
    let sts = rusoto_sts::StsClient::new(rusoto_core::Region::UsEast1);
    let provider = rusoto_sts::StsAssumeRoleSessionCredentialsProvider::new(
        sts,
        "arn:aws:sts::125163634912:role/soup".to_owned(),
        "lobsters-benchmark".to_owned(),
        None,
        None,
        None,
        None,
    );

    b.set_max_duration(3);
    b.wait_limit(time::Duration::from_secs(2 * 60));

    // if the user wants us to terminate, finish whatever we're currently doing first
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    if let Err(e) = ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }) {
        eprintln!("==> failed to set ^C handler: {}", e);
    }

    let scales = args
        .values_of("SCALE")
        .map(|it| it.map(|s| s.parse().unwrap()).collect())
        .unwrap_or_else(|| {
            vec![
                100usize, 500, 1000, 1250, 1500, 1750, 2000, 2500, 2750, 3000, 3500, 4500, 5500,
                6500, 7000, 7500, 8000, 8500, 9000, 9500, 10_000,
            ]
        });

    let memlimit = args.value_of("memory_limit");

    let mut load = if args.is_present("SCALE") {
        OpenOptions::new()
            .write(true)
            .truncate(false)
            .append(true)
            .create(true)
            .open("load.log")
            .unwrap()
    } else {
        let mut f = File::create("load.log").unwrap();
        f.write_all(b"#scale backend sload1 sload5 cload1 cload5\n")
            .unwrap();
        f
    };
    b.run_as(provider, |mut vms: HashMap<String, Vec<Machine>>| {
        use chrono::prelude::*;

        let server = vms.remove("server").unwrap().swap_remove(0);
        let trawler = vms.remove("trawler").unwrap().swap_remove(0);

        // write out host files for ergonomic ssh
        let r: io::Result<()> = try {
            let mut f = File::create("server.host")?;
            writeln!(f, "ubuntu@{}", server.public_dns)?;

            let mut f = File::create("client.host")?;
            writeln!(f, "ubuntu@{}", trawler.public_dns)?;
        };

        if let Err(e) = r {
            eprintln!("failed to write out host files: {:?}", e);
        }

        let shards = [0, 1, 2];

        // allow reuse of time-wait ports
        trawler
            .ssh
            .as_ref()
            .unwrap()
            .just_exec(
                &[
                    "echo",
                    "1",
                    "|",
                    "sudo",
                    "tee",
                    "/proc/sys/net/ipv4/tcp_tw_reuse",
                ],
                "client",
            )?
            .map_err(failure::err_msg)?;

        for &nshard in &shards {
            let mut survived_last = true;
            for &scale in &scales {
                if !survived_last {
                    break;
                }

                let backend = if nshard == 0 {
                    "direct".to_owned()
                } else {
                    format!("direct_{}", nshard)
                };

                eprintln!(
                    "{}",
                    Paint::green(format!("==> benchmark {} at {}x scale", backend, scale)).bold()
                );

                let prefix = format!("lobsters-{}-{}", backend, scale);
                let mut server_chan = None;

                let run: Result<_, failure::Error> = try {
                    // just to make totally sure
                    server.ssh.as_ref().unwrap().exec_print_nonempty(
                        &["pkill", "-9", "-f", "noria-server", "2>&1"],
                        "server",
                    )?;

                    // XXX: also delete log files if we later run with RocksDB?
                    server.ssh.as_ref().unwrap().exec_print_nonempty(
                        &[
                            "target/release/noria-zk",
                            "--clean",
                            "--deployment",
                            "trawler",
                        ],
                        "server",
                    )?;

                    // Don't hit Noria listening timeout think
                    thread::sleep(time::Duration::from_secs(10));

                    // start server again
                    let shards = format!("{}", nshard);
                    let mut cmd = vec![
                        "env",
                        "RUST_BACKTRACE=1",
                        "target/release/noria-server",
                        "--deployment",
                        "trawler",
                        "--durability",
                        "memory",
                        "--no-reuse",
                        "--address",
                        &server.private_ip,
                        "--shards",
                        &shards,
                        "-v",
                        "2>&1",
                        "|",
                        "tee",
                        "server.log",
                    ];
                    if let Some(memlimit) = memlimit {
                        cmd.extend(&["--memory", memlimit]);
                    }

                    server_chan = Some(server.ssh.as_ref().unwrap().exec(&cmd[..], "server")?);

                    // give noria a chance to start
                    thread::sleep(time::Duration::from_secs(5));

                    // run priming
                    eprintln!(
                        "{}",
                        Paint::new(format!(
                            "--> priming at {}",
                            Local::now().time().format("%H:%M:%S")
                        ))
                        .bold()
                    );
                    let in_flight = format!("{}", in_flight);

                    let zk = format!("{}:2181", server.private_ip);
                    let scale = format!("{}", scale);
                    trawler.ssh.as_ref().unwrap().exec_print_nonempty(
                        &[
                            "env",
                            "RUST_BACKTRACE=1",
                            "target/release/lobsters-noria",
                            "--scale",
                            &scale,
                            "--warmup",
                            "0",
                            "--runtime",
                            "0",
                            "--prime",
                            "--in-flight",
                            &in_flight,
                            "-z",
                            &zk,
                            "--deployment",
                            "trawler",
                            "2>&1",
                            "|",
                            "tee",
                            "client.log",
                        ],
                        "client",
                    )?;

                    eprintln!(
                        "{}",
                        Paint::new(format!(
                            "--> warming at {}",
                            Local::now().time().format("%H:%M:%S")
                        ))
                        .bold()
                    );

                    trawler.ssh.as_ref().unwrap().exec_print_nonempty(
                        &[
                            "env",
                            "RUST_BACKTRACE=1",
                            "target/release/lobsters-noria",
                            "--scale",
                            &scale,
                            "--warmup",
                            "30",
                            "--runtime",
                            "0",
                            "--in-flight",
                            &in_flight,
                            "-z",
                            &zk,
                            "--deployment",
                            "trawler",
                            "2>&1",
                            "|",
                            "tee",
                            "client.log",
                        ],
                        "client",
                    )?;

                    eprintln!(
                        "{}",
                        Paint::new(format!(
                            "--> started at {}",
                            Local::now().time().format("%H:%M:%S")
                        ))
                        .bold()
                    );

                    let mut output = File::create(format!("{}.log", prefix))?;
                    let hist_output = if let Some(memlimit) = memlimit {
                        format!(
                            "--histogram=lobsters-{}-r{}-l{}.hist ",
                            backend, scale, memlimit
                        )
                    } else {
                        format!(
                            "--histogram=lobsters-{}-r{}-unlimited.hist ",
                            backend, scale
                        )
                    };
                    let res = trawler.ssh.as_ref().unwrap().just_exec(
                        &[
                            "env",
                            "RUST_BACKTRACE=1",
                            "target/release/lobsters-noria",
                            "--scale",
                            &scale,
                            "--warmup",
                            "15",
                            "--runtime",
                            "30",
                            "--in-flight",
                            &in_flight,
                            "-z",
                            &zk,
                            "--deployment",
                            "trawler",
                            &hist_output,
                            "2>&1",
                            "|",
                            "tee",
                            "client.log",
                        ],
                        "client",
                    )?;

                    match res {
                        Ok(ref result) | Err(ref result) => {
                            output.write_all(result.as_bytes())?;
                        }
                    }
                    drop(output);

                    let _ = res.map_err(|_| failure::err_msg("client failed"))?;

                    // gather server load
                    let sload = server
                        .ssh
                        .as_ref()
                        .unwrap()
                        .just_exec(&["awk", "{print $1\" \"$2}", "/proc/loadavg"], "server")?
                        .map_err(failure::err_msg)?;
                    let sload = sload.trim_end();

                    // gather client load
                    let cload = trawler
                        .ssh
                        .as_ref()
                        .unwrap()
                        .just_exec(&["awk", "{print $1\" \"$2}", "/proc/loadavg"], "client")?
                        .map_err(failure::err_msg)?;
                    let cload = cload.trim_end();

                    load.write_all(format!("{} {} ", scale, backend).as_bytes())?;
                    load.write_all(sload.as_bytes())?;
                    load.write_all(b" ")?;
                    load.write_all(cload.as_bytes())?;
                    load.write_all(b"\n")?;

                    let mut hist = File::create(format!("{}.hist", prefix))?;
                    let hist_cmd = if let Some(memlimit) = memlimit {
                        format!("cat lobsters-{}-r{}-l{}.hist", backend, scale, memlimit)
                    } else {
                        format!("cat lobsters-{}-r{}-unlimited.hist", backend, scale)
                    };
                    trawler
                        .ssh
                        .as_ref()
                        .unwrap()
                        .cmd_raw(&hist_cmd)
                        .and_then(|out| Ok(hist.write_all(&out[..]).map(|_| ())?))?;

                    let sload: f64 = sload
                        .split_whitespace()
                        .next()
                        .and_then(|l| l.parse().ok())
                        .unwrap_or(0.0);
                    let cload: f64 = cload
                        .split_whitespace()
                        .next()
                        .and_then(|l| l.parse().ok())
                        .unwrap_or(0.0);
                    (sload, cload)
                };

                let erred = run.is_err();
                if erred {
                    eprintln!(
                        "{}",
                        Paint::red(format!(
                            "--> errored at {}",
                            Local::now().time().format("%H:%M:%S")
                        ))
                        .bold()
                    );
                } else {
                    eprintln!(
                        "{}",
                        Paint::new(format!(
                            "--> finished at {}",
                            Local::now().time().format("%H:%M:%S")
                        ))
                        .bold()
                    );
                }

                // attempt cleanup
                let cleanup: Result<(), failure::Error> = try {
                    // stop old server
                    // gather state size
                    let mem_limit = if let Some(limit) = memlimit {
                        format!("l{}", limit)
                    } else {
                        "unlimited".to_owned()
                    };
                    let mut sizefile = File::create(format!(
                        "lobsters-{}-r{}-{}.json",
                        backend, scale, mem_limit
                    ))?;
                    trawler
                        .ssh
                        .as_ref()
                        .unwrap()
                        .cmd_raw(&format!(
                            "wget http://{}:9000/get_statistics",
                            server.private_ip
                        ))
                        .and_then(|out| Ok(sizefile.write_all(&out[..]).map(|_| ())?))?;

                    // stop the server
                    let _ = server
                        .ssh
                        .as_ref()
                        .unwrap()
                        .just_exec(&["pkill", "-f", "noria-server", "2>&1"], "server")?;
                    if let Some(mut server_chan) = server_chan {
                        let server_stdout = finalize(&mut server_chan)?;
                        if erred {
                            let erred = server_stdout.is_err();
                            let server_stdout = server_stdout.unwrap_or_else(|e| e);
                            for line in server_stdout.lines() {
                                let mut paint = Paint::new("server").dimmed();
                                if erred {
                                    paint = paint.fg(yansi::Color::Red);
                                }
                                eprintln!(
                                    "{:6} {}",
                                    paint,
                                    Paint::new(format!("| {}", line)).dimmed()
                                );
                            }
                        }
                    }
                };

                // also parse achived ops/s to check that we're *really* keeping up
                if let Ok(log) = File::open(format!("{}.log", prefix)) {
                    let log = BufReader::new(log);
                    let mut target = None;
                    let mut actual = None;
                    for line in log.lines() {
                        let line = line?;
                        if line.starts_with("# target ops/s") {
                            target = Some(line.rsplitn(2, ' ').next().unwrap().parse::<f64>()?);
                        } else if line.starts_with("# generated ops/s") {
                            actual = Some(line.rsplitn(2, ' ').next().unwrap().parse::<f64>()?);
                        }
                        match (target, actual) {
                            (Some(target), Some(actual)) => {
                                eprintln!(
                                    "{}",
                                    Paint::cyan(format!(
                                        " -> generated {} ops/s (target: {})",
                                        actual, target
                                    ))
                                );
                                if actual < target * 4.0 / 5.0 {
                                    eprintln!(
                                        "{}",
                                        Paint::red(" -> backend is really not keeping up").bold()
                                    );
                                    survived_last = false;
                                }
                                break;
                            }
                            _ => {}
                        }
                    }
                }

                // stop iterating through scales for this backend if it's not keeping up
                let (sload, cload) = run?;

                eprintln!(
                    "{}",
                    Paint::cyan(format!(
                        " -> backend load: s: {}/16, c: {}/48",
                        sload, cload
                    ))
                );

                if sload > 16.5 {
                    eprintln!(
                        "{}",
                        Paint::yellow(" -> backend is probably not keeping up").bold()
                    );
                }

                let _ = cleanup?;

                if !running.load(Ordering::SeqCst) {
                    // user pressed ^C
                    break;
                }
            }

            if !running.load(Ordering::SeqCst) {
                // user pressed ^C
                break;
            }
        }

        Ok(())
    })
    .unwrap();
}

fn finalize(c: &mut ssh2::Channel) -> Result<Result<String, String>, failure::Error> {
    // sending EOF may fail if the remote command has failed
    // that's okay
    let _ = c.send_eof();

    let mut stdout = Vec::new();
    while !c.eof() {
        c.read_to_end(&mut stdout)?;
    }
    c.wait_close()?;
    let stdout = String::from_utf8(stdout)?;

    if c.exit_status()? != 0 {
        return Ok(Err(stdout));
    }
    Ok(Ok(stdout))
}

impl ConvenientSession for tsunami::Session {
    fn exec(&self, cmd: &[&str], on: &str) -> Result<ssh2::Channel, failure::Error> {
        let cmd: Vec<_> = cmd
            .iter()
            .map(|&arg| match arg {
                "&" | "&&" | "<" | ">" | "2>" | "2>&1" | "|" => arg.to_string(),
                arg if arg.starts_with(">(") => arg.to_string(),
                arg if arg.starts_with("!") => arg[1..].to_string(),
                _ => shellwords::escape(arg),
            })
            .collect();
        let cmd = cmd.join(" ");
        eprintln!("{}", Paint::blue(format!("{} $ {}", on, cmd)));

        // ensure we're using a Bourne shell (that's what shellwords supports too)
        let cmd = format!("bash -c {}", shellwords::escape(&cmd));

        let mut c = self.channel_session()?;
        c.exec(&cmd)?;
        Ok(c)
    }
    fn just_exec(&self, cmd: &[&str], on: &str) -> Result<Result<String, String>, failure::Error> {
        let mut c = self.exec(cmd, on)?;
        finalize(&mut c)
    }
    fn exec_print_nonempty(&self, cmd: &[&str], on: &str) -> Result<(), failure::Error> {
        let r = self.just_exec(cmd, on)?;
        let mut erred = r.is_err();
        match r {
            Ok(stdout) | Err(stdout) => {
                if stdout.contains("panicked at") {
                    erred = true;
                }

                let out = stdout.trim_end();
                if erred || (!out.is_empty() && out != "Already up to date.") {
                    for line in out.lines() {
                        let mut paint = Paint::new(on).dimmed();
                        if erred {
                            paint = paint.fg(yansi::Color::Red);
                        }
                        eprintln!("{:6} {}", paint, Paint::new(format!("| {}", line)).dimmed());
                    }
                }

                if erred {
                    Err(failure::err_msg("command exited with an error")
                        .context(cmd.join(" "))
                        .into())
                } else {
                    Ok(())
                }
            }
        }
    }
}

trait ConvenientSession {
    fn exec(&self, cmd: &[&str], on: &str) -> Result<ssh2::Channel, failure::Error>;
    fn just_exec(&self, cmd: &[&str], on: &str) -> Result<Result<String, String>, failure::Error>;
    fn exec_print_nonempty(&self, cmd: &[&str], on: &str) -> Result<(), failure::Error>;
}
