use egutil::db::DatabaseConnection;
use getopts::Options;
use log::{error, info};
use std::env;
use threadpool::ThreadPool;

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

fn main() {
    env_logger::init();

    if let Some((options, mut connection)) = init() {
        connection.connect();

        let sql = create_sql(&options);
        let ids = get_record_ids(&mut connection, &sql);
    }
}
