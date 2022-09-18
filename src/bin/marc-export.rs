use egutil::db::DatabaseConnection;
use getopts;
use marcutil::Record;
use std::io::prelude::*;
use std::{env, fs, io};

struct ExportOptions {
    min_id: i64,
    max_id: i64,
    newest_first: bool,
    destination: ExportDestination,
    query_file: Option<String>,
}

enum ExportDestination {
    Stdout,
    File(String),
}

fn read_options() -> Option<(ExportOptions, DatabaseConnection)> {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optopt("", "db-host", "Database Host", "DB_HOST");
    opts.optopt("", "db-port", "Database Port", "DB_PORT");
    opts.optopt("", "db-user", "Database User", "DB_USER");
    opts.optopt("", "db-name", "Database Name", "DB_NAME");

    opts.optopt("", "min-id", "Minimum record ID", "MIN_REC_ID");
    opts.optopt("", "max-id", "Maximum record ID", "MAX_REC_ID");
    opts.optopt("", "out-file", "Output File", "OUTPUT_FILE");
    opts.optopt("", "query-file", "SQL Query File", "query_file");

    opts.optflag("", "newest-first", "Newest First");
    opts.optflag("h", "help", "Help");

    let params = opts.parse(&args[1..]).unwrap();

    if params.opt_present("help") {
        print_help();
        return None;
    }

    let destination = match params.opt_get::<String>("out-file").unwrap() {
        Some(filename) => ExportDestination::File(filename),
        None => ExportDestination::Stdout,
    };

    let mut builder = DatabaseConnection::builder();
    builder.set_opts(&params);
    let connection = builder.build();

    Some((
        ExportOptions {
            destination,
            min_id: params.opt_get_default("min-id", -1).unwrap(),
            max_id: params.opt_get_default("max-id", -1).unwrap(),
            newest_first: params.opt_present("newest-first"),
            query_file: params.opt_get("query-file").unwrap(),
        },
        connection,
    ))
}

fn print_help() {
    println!(
        r#"

Synopsis

    cargo run -- --out-file /tmp/records.mrc

Options

    --min-id
        Only export records whose ID is >= this value.

    --max-id
        Only export records whose ID is <= this value.

    --out-file
        Write data to this file.
        Otherwise, writes to STDOUT.

    --query-file
        Path to a file containing an SQL query.  The query must
        produce rows that have a column named "marc".

    --newest-first
        Export records newest to oldest by create date.
        Otherwise, export oldests to newest.

    --help Print help message

    "#
    );
}

fn create_sql(ops: &ExportOptions) -> String {
    if let Some(fname) = &ops.query_file {
        return fs::read_to_string(fname).unwrap();
    }

    let select = "SELECT bre.marc";
    let from = "FROM biblio.record_entry bre";
    let mut filter = String::from("WHERE NOT bre.deleted");

    if ops.min_id > -1 {
        filter = format!("{} AND id >= {}", filter, ops.min_id);
    }

    if ops.max_id > -1 {
        filter = format!("{} AND id < {}", filter, ops.max_id);
    }

    let order_by = match ops.newest_first {
        true => "ORDER BY create_date DESC",
        false => "ORDER BY create_date ASC",
    };

    format!("{select} {from} {filter} {order_by}")
}

fn export(con: &mut DatabaseConnection, ops: &ExportOptions) -> Result<(), String> {
    let mut writer: Box<dyn Write> = match &ops.destination {
        ExportDestination::File(fname) => Box::new(fs::File::create(fname).unwrap()),
        _ => Box::new(io::stdout()),
    };

    con.connect()?;

    let query = create_sql(ops);

    for row in con.client().query(&query[..], &[]).unwrap() {
        let marc_xml: &str = row.get("marc");

        let record = Record::from_xml(&marc_xml).next().unwrap();
        let binary = record.to_binary().unwrap();

        writer.write(&binary).unwrap();
    }

    con.disconnect();

    Ok(())
}

fn main() -> Result<(), String> {
    if let Some((options, mut connection)) = read_options() {
        export(&mut connection, &options)
    } else {
        Ok(())
    }
}
