//! Protocol fixture only; it does not load a model or perform inference.
use std::{env, fs};

#[derive(serde::Deserialize)]
struct Input {
    requests: Vec<Row>,
}
#[derive(serde::Deserialize)]
struct Row {
    id: String,
    text: String,
}
#[derive(serde::Serialize)]
struct Output {
    embeddings: Vec<Vector>,
}
#[derive(serde::Serialize)]
struct Vector {
    id: String,
    values: Vec<f32>,
}

fn main() {
    let mut input = None;
    let mut output = None;
    let mut sleep_millis = None;
    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--input" => input = args.next(),
            "--output" => output = args.next(),
            "--sleep-millis" => sleep_millis = args.next().and_then(|value| value.parse().ok()),
            _ => {}
        }
    }
    let (Some(input), Some(output)) = (input, output) else {
        std::process::exit(2)
    };
    if let Some(milliseconds) = sleep_millis {
        std::thread::sleep(std::time::Duration::from_millis(milliseconds));
    }
    let input: Input =
        serde_json::from_slice(&fs::read(input).expect("fixture input")).expect("fixture protocol");
    let embeddings = input
        .requests
        .into_iter()
        .map(|row| Vector {
            id: row.id,
            // Deliberately protocol-only fixture output, not an embedding model.
            values: vec![row.text.len() as f32, 1.0],
        })
        .collect();
    fs::write(output, serde_json::to_vec(&Output { embeddings }).unwrap()).expect("fixture output");
}
