use std::process;

fn main() {
    if let Err(err) = ceptra::app::run() {
        eprintln!("fatal: {err}");
        process::exit(1);
    }
}
