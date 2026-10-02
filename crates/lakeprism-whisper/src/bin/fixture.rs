//! Protocol fixture only. It proves the adapter contract without a model.
use std::env;
use std::fs::File;
use std::io::Write;

fn main() {
    let mut output = None;
    let mut sleep_millis = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--output" {
            output = args.next();
        } else if arg == "--sleep-millis" {
            sleep_millis = args.next().and_then(|value| value.parse::<u64>().ok());
        }
    }
    let Some(output) = output else {
        std::process::exit(2)
    };
    if let Some(sleep_millis) = sleep_millis {
        std::thread::sleep(std::time::Duration::from_millis(sleep_millis));
    }
    let mut file = File::create(output).expect("fixture output");
    file.write_all(br#"{"segments":[{"start_millis":0,"end_millis":500,"text":"fixture transcript","confidence_millis":987}]}"#)
        .expect("fixture write");
}
