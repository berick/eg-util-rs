use egutil::db::DatabaseConnection;
use getopts::Options;
use log::{debug, error, info};
use std::env;
use std::thread;
use threadpool::ThreadPool;

#[derive(Debug, Clone)]
struct IngestOptions {
    max_threads: u8,
    do_browse: bool,
    do_attrs: bool,
    do_search: bool,
    do_facets: bool,
    do_display: bool,
    min_id: usize,
    max_id: usize,
    newest_first: bool,
    batch_size: usize,
    attrs: Vec<String>,
}

/// Read command line options and setup our database connection.
fn init() -> Option<(IngestOptions, DatabaseConnection)> {
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();

    opts.optopt("", "db-host", "Database Host", "DB_HOST");
    opts.optopt("", "db-port", "Database Port", "DB_PORT");
    opts.optopt("", "db-user", "Database User", "DB_USER");
    opts.optopt("", "db-name", "Database Name", "DB_NAME");

    opts.optopt("", "max-threads", "Max Worker Threads", "MAX_THREADS");
    opts.optopt(
        "",
        "batch-size",
        "Number of Records to Process per Batch",
        "BATCH_SIZE",
    );
    opts.optopt("", "min-id", "Minimum Record ID", "MIN_REC_ID");
    opts.optopt("", "max-id", "Maximum Record ID", "MAX_REC_ID");
    opts.optmulti("", "attr", "Reingest Specific Attribute, Repetable", "RECORD_ATTR");

    opts.optflag("h", "help", "Show Help Text");
    opts.optflag("", "do-browse", "Update Browse");
    opts.optflag("", "do-attrs", "Update Record Attributes");
    opts.optflag("", "do-search", "Update Search Indexes");
    opts.optflag("", "do-facets", "Update Facets");
    opts.optflag("", "do-display", "Update Display Fields");
    opts.optflag("", "newest-first", "Update Records Newest to Oldest");

    let params = match opts.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => {
            error!("\nError processing options: {e}");
            println!("{}", opts.usage("Usage: "));
            return None;
        }
    };

    if params.opt_present("help") {
        println!("{}", opts.usage("Usage: "));
        return None;
    }

    let ingest_ops = IngestOptions {
        max_threads: params.opt_get_default("max-threads", 5).unwrap(),
        do_browse: params.opt_present("do-browse"),
        do_attrs: params.opt_present("do-attrs"),
        do_search: params.opt_present("do-search"),
        do_facets: params.opt_present("do-facets"),
        do_display: params.opt_present("do-display"),
        min_id: params.opt_get_default("min-id", 0).unwrap(),
        max_id: params.opt_get_default("max-id", 0).unwrap(),
        newest_first: params.opt_present("newest-first"),
        batch_size: params.opt_get_default("batch-size", 100).unwrap(),
        attrs: params.opt_strs("attr"),
    };

    let mut builder = DatabaseConnection::builder();
    builder.set_opts(&params);
    let connection = builder.build();

    Some((ingest_ops, connection))
}

fn create_sql(options: &IngestOptions) -> String {
    let select = "SELECT id FROM biblio.record_entry";
    let mut filter = format!("WHERE NOT deleted AND id > {}", options.min_id);

    if options.max_id > 0 {
        filter += &format!(" AND id < {}", options.max_id);
    }

    let mut order_by;
    if options.newest_first {
        order_by = "ORDER BY create_date DESC, id DESC";
    } else {
        order_by = "ORDER BY id";
    }

    format!("{select} {filter} {order_by}")
}

fn get_record_ids(connection: &mut DatabaseConnection, sql: &str) -> Vec<i64> {
    let mut ids = Vec::new();

    for row in connection.client().query(&sql[..], &[]).unwrap() {
        let id: i64 = row.get("id");
        ids.push(id);
    }

    info!("Found {} record IDs to process", ids.len());

    ids
}

fn ingest_records(
    options: &IngestOptions,
    connection: &mut DatabaseConnection,
    ids: &mut Vec<i64>,
) {
    let pool = ThreadPool::new(options.max_threads as usize);

    loop {
        let end = match ids.len() {
            0 => break,
            n if n >= options.batch_size => options.batch_size,
            _ => ids.len(),
        };

        // Always pull from index 0 since we are draining the Vec each time.
        let batch: Vec<i64> = ids.drain(0..end).collect();

        let ops = options.clone();
        let mut con = connection.partial_clone();

        pool.execute(move || process_batch(ops, con, batch));
    }

    pool.join();
}

/// Start point for our threads
fn process_batch(options: IngestOptions, mut connection: DatabaseConnection, ids: Vec<i64>) {
    connection.connect().unwrap();

    if options.do_attrs {
        reingest_attributes(&options, &mut connection, &ids);
    }
}

fn reingest_attributes(
    options: &IngestOptions,
    connection: &mut DatabaseConnection,
    ids: &Vec<i64>,
) {
    info!(
        "Thread {:?} processing {} records",
        thread::current().id(),
        ids.len()
    );

    let mut sql = r#"
        SELECT metabib.reingest_record_attributes($1)
        FROM biblio.record_entry
        WHERE id = $2
    "#;

    if options.attrs.len() > 0 {

        let sql = r#"
            SELECT metabib.reingest_record_attributes($1, $3)
            FROM biblio.record_entry
            WHERE id = $2
        "#;

        let stmt = connection.client().prepare(sql).unwrap();

        for id in ids {
            if let Err(e) =
                connection.client().query(&stmt, &[id, id, &options.attrs.as_slice()]) {
                error!("Error processing record: {id} {e}");
            }
        }

    } else {

        let sql = r#"
            SELECT metabib.reingest_record_attributes($1)
            FROM biblio.record_entry
            WHERE id = $2
        "#;

        let stmt = connection.client().prepare(sql).unwrap();

        for id in ids {
            if let Err(e) = connection.client().query(&stmt, &[id, id]) {
                error!("Error processing record: {id} {e}");
            }
        }
    }
}

fn main() {
    env_logger::init();

    let (options, mut connection) = match init() {
        Some((o, c)) => (o, c),
        None => return,
    };

    connection.connect();

    let sql = create_sql(&options);
    let mut ids = get_record_ids(&mut connection, &sql);

    // Future DB interactions will be per-thread.
    connection.disconnect();

    ingest_records(&options, &mut connection, &mut ids);
}
