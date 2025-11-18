use ceptra::{BootstrapController, PodIdentity};
use serde::Deserialize;
use std::env;
use std::fs;
use std::process;

#[derive(Debug, Deserialize)]
struct PodSpec {
    name: String,
    ordinal: usize,
    ready: bool,
}

#[derive(Debug, Deserialize)]
struct BootstrapSpec {
    cp_quorum: usize,
    partitions: Vec<String>,
    pods: Vec<PodSpec>,
}

fn main() {
    let mut args = env::args().skip(1);
    let Some(cmd) = args.next() else {
        return usage();
    };
    match cmd.as_str() {
        "plan" => {
            let path = args.next().unwrap_or_else(|| abort("missing spec path"));
            run_plan(&path);
        }
        "pre-stop" => {
            let path = args.next().unwrap_or_else(|| abort("missing spec path"));
            let pod = args.next().unwrap_or_else(|| abort("missing pod name"));
            let leader = args
                .next()
                .unwrap_or_else(|| abort("missing current leader name"));
            let epoch = args
                .next()
                .unwrap_or_else(|| abort("missing epoch"))
                .parse::<u64>()
                .unwrap_or_else(|_| abort("epoch must be a number"));
            run_pre_stop(&path, &pod, &leader, epoch);
        }
        _ => usage(),
    }
}

fn usage() {
    eprintln!("usage:");
    eprintln!("  bootstrapper plan <spec.json>");
    eprintln!("  bootstrapper pre-stop <spec.json> <pod> <current_leader> <epoch>");
    process::exit(1);
}

fn abort(msg: &str) -> ! {
    eprintln!("error: {msg}");
    process::exit(1);
}

fn load_spec(path: &str) -> BootstrapSpec {
    let data = fs::read_to_string(path).unwrap_or_else(|_| abort("failed to read spec"));
    serde_json::from_str(&data).unwrap_or_else(|_| abort("failed to parse spec"))
}

fn run_plan(path: &str) {
    let spec = load_spec(path);
    let pods = spec
        .pods
        .into_iter()
        .map(|pod| PodIdentity::new(pod.name, pod.ordinal, pod.ready))
        .collect();
    let plan = BootstrapController::plan(pods, spec.cp_quorum, spec.partitions);
    let json = serde_json::to_string_pretty(&plan).unwrap();
    println!("{json}");
}

fn run_pre_stop(path: &str, pod: &str, leader: &str, epoch: u64) {
    let spec = load_spec(path);
    let pods: Vec<PodIdentity> = spec
        .pods
        .into_iter()
        .map(|pod| PodIdentity::new(pod.name, pod.ordinal, pod.ready))
        .collect();
    let action = BootstrapController::plan_pre_stop(&pods, pod, leader, epoch);
    let json = serde_json::to_string_pretty(&action).unwrap();
    println!("{json}");
}
