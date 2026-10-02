use std::io::{self, BufRead, Write};
use std::sync::Arc;

use futures::TryStreamExt;
use lakeprism_cli::{
    CliError, LocalCli, OutputFormat, default_catalog_root, format_audit, format_status,
    parse_loopback_address, write_batches,
};
use lakeprism_core::StorageMode;

#[tokio::main]
async fn main() {
    if let Err(error) = run(std::env::args().skip(1).collect()).await {
        eprintln!("lakeprism: {error}");
        std::process::exit(2);
    }
}

async fn run(mut args: Vec<String>) -> Result<(), CliError> {
    let catalog = take_option(&mut args, "--catalog")
        .map(Into::into)
        .unwrap_or_else(default_catalog_root);
    let command = args.first().cloned().unwrap_or_else(|| "shell".to_owned());
    if !args.is_empty() {
        args.remove(0);
    }
    let mut cli = LocalCli::open(catalog).await?;
    match command.as_str() {
        "init" => println!(
            "initialized local catalog at {}",
            cli.catalog_root().display()
        ),
        "register" => register(&mut cli, &args).await?,
        "ddl" => {
            let statement = one_value("ddl", &args)?;
            println!("{:?}", cli.ddl(&statement).await?);
        }
        "sql" => sql(&cli, &args, false).await?,
        "explain-media" => sql(&cli, &args, true).await?,
        "query-status" => {
            let id = one_value("query-status", &args)?;
            match cli.query_status(&id) {
                Some(info) => println!("{}", format_status(&info)),
                None => return Err(CliError::Usage(format!("query not found: {id}"))),
            }
        }
        "query-audit" => {
            let id = one_value("query-audit", &args)?;
            match cli.query_audit(&id) {
                Some(event) => println!("{}", format_audit(&event)),
                None => return Err(CliError::Usage(format!("query not found: {id}"))),
            }
        }
        "query-cancel" => {
            let id = one_value("query-cancel", &args)?;
            if !cli.cancel_query(&id) {
                return Err(CliError::Usage(format!("query is not cancellable: {id}")));
            }
            println!("cancel requested for {id}");
        }
        "flight" => {
            let address =
                take_option(&mut args, "--addr").unwrap_or_else(|| "127.0.0.1:5005".to_owned());
            let address = parse_loopback_address(&address)?;
            println!("serving local Flight SQL at http://{address}");
            cli.serve_flight(address).await?;
        }
        "shell" => shell(cli).await?,
        "help" | "--help" | "-h" => print_help(),
        other => return Err(CliError::Usage(format!("unknown command: {other}"))),
    }
    Ok(())
}

async fn register(cli: &mut LocalCli, args: &[String]) -> Result<(), CliError> {
    if args.len() != 3 && args.len() != 4 {
        return Err(CliError::Usage(
            "register requires TABLE URI MEDIA_TYPE [external|managed|inline]".to_owned(),
        ));
    }
    let storage = match args.get(3).map(String::as_str).unwrap_or("external") {
        "external" => StorageMode::External,
        "managed" => StorageMode::Managed,
        "inline" => StorageMode::Inline,
        value => {
            return Err(CliError::Usage(format!(
                "invalid storage mode {value:?}; use external, managed, or inline"
            )));
        }
    };
    cli.register_media(&args[0], &args[1], &args[2], storage)
        .await?;
    println!("registered {} in {}", args[1], args[0]);
    Ok(())
}

async fn sql(cli: &LocalCli, args: &[String], explain_media: bool) -> Result<(), CliError> {
    let mut args = args.to_vec();
    let format = take_option(&mut args, "--format")
        .map(|value| OutputFormat::parse(&value))
        .transpose()?
        .unwrap_or(OutputFormat::Json);
    let statement = one_value(
        if explain_media {
            "explain-media"
        } else {
            "sql"
        },
        &args,
    )?;
    let batches = if explain_media {
        eprintln!(
            "EXPLAIN MEDIA reports the local DataFusion plan; index/cold-work selection uses MediaSession::explain_media_transcript_search."
        );
        cli.explain_media(&statement).await?
    } else {
        let (id, batches) = cli.query_batches(&statement).await?;
        eprintln!("query_id={id}");
        batches
    };
    write_batches(&mut io::stdout().lock(), &batches, format)
}

async fn shell(mut cli: LocalCli) -> Result<(), CliError> {
    let history = cli.catalog_root().join("history");
    let mut saved_history = std::fs::read_to_string(&history).unwrap_or_default();
    println!("LakePrism local shell. Type \\help for commands.");
    let input = io::stdin();
    for line in input.lock().lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saved_history.push_str(trimmed);
        saved_history.push('\n');
        std::fs::write(&history, &saved_history)?;
        if trimmed == "\\quit" || trimmed == "\\q" {
            break;
        }
        if trimmed == "\\help" {
            print_shell_help();
            continue;
        }
        if trimmed == "\\tables" {
            for table in cli.session().catalog_tables().await? {
                println!(
                    "{}.{}.{}",
                    table.catalog_name, table.schema_name, table.table_name
                );
            }
            continue;
        }
        if trimmed == "\\history" {
            print!("{saved_history}");
            continue;
        }
        if let Some(id) = trimmed.strip_prefix("\\status ") {
            match cli.query_status(id.trim()) {
                Some(info) => println!("{}", format_status(&info)),
                None => println!("query not found: {}", id.trim()),
            }
            continue;
        }
        if let Some(id) = trimmed.strip_prefix("\\audit ") {
            match cli.query_audit(id.trim()) {
                Some(event) => println!("{}", format_audit(&event)),
                None => println!("query not found: {}", id.trim()),
            }
            continue;
        }
        if let Some(id) = trimmed.strip_prefix("\\cancel ") {
            println!("cancelled={}", cli.cancel_query(id.trim()));
            continue;
        }
        if let Some(statement) = trimmed.strip_prefix("\\ddl ") {
            println!("{:?}", cli.ddl(statement).await?);
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("\\register ") {
            let args = rest
                .split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            register(&mut cli, &args).await?;
            continue;
        }
        if let Some(statement) = trimmed.strip_prefix("\\run ") {
            let id = cli.session().create_query(None);
            let session = Arc::clone(cli.session());
            let statement = statement.to_owned();
            let task_id = id.clone();
            tokio::spawn(async move {
                match session
                    .execute_registered_query(&statement, task_id.clone())
                    .await
                {
                    Ok(execution) => match execution.stream.try_collect::<Vec<_>>().await {
                        Ok(batches) => {
                            let mut output = io::stdout().lock();
                            if let Err(error) =
                                write_batches(&mut output, &batches, OutputFormat::Json)
                            {
                                eprintln!("query_id={task_id}; output error: {error}");
                            }
                        }
                        Err(error) => eprintln!("query_id={task_id}; execution error: {error}"),
                    },
                    Err(error) => eprintln!("query_id={task_id}; execution error: {error}"),
                }
            });
            println!("query_id={id}");
            continue;
        }
        if let Some(statement) = trimmed.strip_prefix("EXPLAIN MEDIA ") {
            let batches = cli.explain_media(statement).await?;
            write_batches(&mut io::stdout().lock(), &batches, OutputFormat::Json)?;
            continue;
        }
        let (_, batches) = cli.query_batches(trimmed).await?;
        write_batches(&mut io::stdout().lock(), &batches, OutputFormat::Json)?;
        io::stdout().flush()?;
    }
    Ok(())
}

fn one_value(name: &str, values: &[String]) -> Result<String, CliError> {
    if values.len() == 1 {
        Ok(values[0].clone())
    } else {
        Err(CliError::Usage(format!(
            "{name} requires exactly one quoted value"
        )))
    }
}

fn take_option(args: &mut Vec<String>, name: &str) -> Option<String> {
    let position = args.iter().position(|value| value == name)?;
    if position + 1 >= args.len() {
        return None;
    }
    args.remove(position);
    Some(args.remove(position))
}

fn print_help() {
    println!(
        "lakeprism [--catalog PATH] <command>\n\
         commands:\n\
           init\n\
           register TABLE URI MEDIA_TYPE [external|managed|inline]\n\
           ddl 'CREATE MEDIA TABLE name|DROP TABLE name|SHOW TABLES'\n\
           sql [--format json|csv|arrow] 'SELECT ...'\n\
           explain-media [--format json|csv|arrow] 'SELECT ...'\n\
           query-status ID | query-audit ID | query-cancel ID\n\
           flight [--addr 127.0.0.1:5005]\n\
           shell"
    );
}

fn print_shell_help() {
    println!(
        "\\tables, \\ddl DDL, \\register TABLE URI TYPE [MODE], \\status ID, \\audit ID, \\cancel ID, \
         \\run SQL, \\history, EXPLAIN MEDIA SELECT ..., \\quit\n\
         Other input is local SQL. History is stored in the local catalog directory."
    );
}
