use getopts;
use postgres as pg;
///! Create, connect, and manage database connections.
use std::env;

const DEFAULT_DB_PORT: u16 = 5432;
const DEFAULT_DB_HOST: &str = "localhost";
const DEFAULT_DB_USER: &str = "evergreen";
const DEFAULT_DB_NAME: &str = "evergreen";

/// For compiling a set of connection parameters
///
/// Values are applied like so:
///
/// 1. Manually applying a value via set_* method
/// 2. Values provided via getopts::Matches struct.
/// 3. Values pulled from the environment (e.g. PGHOST) where possible.
/// 4. Default values defined in this module.
pub struct DatabaseConnectionBuilder {
    client: Option<pg::Client>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    database: Option<String>,
    // Name of client application.
    application: Option<String>,
}

impl DatabaseConnectionBuilder {
    pub fn new() -> Self {
        DatabaseConnectionBuilder {
            host: None,
            port: None,
            user: None,
            database: None,
            application: None,
            client: None,
        }
    }

    /// Set connection values via getopts matches.
    ///
    /// Values are only applied where values do not already exist.
    /// This generally means a set_* method has higher precedence
    /// than a set of getopts matches.
    ///
    /// Supported options:
    ///     --db-host
    ///     --db-port
    ///     --db-user
    ///     --db-name
    pub fn set_opts(&mut self, params: &getopts::Matches) {
        if self.host.is_none() {
            if params.opt_defined("db-host") {
                self.host = params.opt_str("db-host");
            }
        }

        if self.user.is_none() {
            if params.opt_defined("db-user") {
                self.user = params.opt_str("db-user");
            }
        }

        if self.database.is_none() {
            if params.opt_defined("db-name") {
                self.database = params.opt_str("db-name");
            }
        }

        if self.port.is_none() {
            if params.opt_defined("db-port") {
                if let Some(v) = params.opt_str("db-port") {
                    self.port = Some(v.parse::<u16>().unwrap());
                }
            }
        }
    }

    pub fn set_host(&mut self, host: &str) {
        self.host = Some(host.to_string())
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = Some(port);
    }

    pub fn set_user(&mut self, user: &str) {
        self.user = Some(user.to_string());
    }

    pub fn set_database(&mut self, database: &str) {
        self.database = Some(database.to_string());
    }

    pub fn set_application(&mut self, application: &str) {
        self.application = Some(application.to_string());
    }

    fn from_env(name: &str) -> Option<String> {
        match env::vars().filter(|(k, _)| k.eq(name)).next() {
            Some((_, v)) => Some(v.to_string()),
            None => None,
        }
    }

    /// Create the final database connection object from the collected
    /// parameters.
    pub fn build(self) -> DatabaseConnection {
        let host = match self.host {
            Some(h) => h,
            None => match DatabaseConnectionBuilder::from_env("PGHOST") {
                Some(h) => h,
                None => DEFAULT_DB_HOST.to_string(),
            },
        };

        let user = match self.user {
            Some(h) => h,
            None => match DatabaseConnectionBuilder::from_env("PGUSER") {
                Some(h) => h,
                None => DEFAULT_DB_USER.to_string(),
            },
        };

        let database = match self.database {
            Some(h) => h,
            None => match DatabaseConnectionBuilder::from_env("PGDATABASE") {
                Some(h) => h,
                None => DEFAULT_DB_NAME.to_string(),
            },
        };

        let port = match self.port {
            Some(h) => h,
            None => match DatabaseConnectionBuilder::from_env("PGPORT") {
                Some(h) => h.parse::<u16>().unwrap(),
                None => DEFAULT_DB_PORT,
            },
        };

        let mut dsn = format!(
            "host={} port={} user={} dbname={}",
            host, port, user, database
        );

        if let Some(ref app) = self.application {
            dsn += &format!(" application={}", &app);
        }

        DatabaseConnection {
            host,
            port,
            user,
            dsn,
            database,
            application: self.application,
            client: None,
        }
    }
}

/// Wrapper for a postgres::Client with connection metadata.
pub struct DatabaseConnection {
    client: Option<pg::Client>,
    dsn: String,
    host: String,
    port: u16,
    user: String,
    database: String,
    application: Option<String>,
}

impl DatabaseConnection {
    pub fn builder() -> DatabaseConnectionBuilder {
        DatabaseConnectionBuilder::new()
    }

    /// Our connection string
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    /// Mutable client ref
    ///
    /// Panics if the client is not yet connected / created.
    pub fn client(&mut self) -> &mut pg::Client {
        if self.client.is_none() {
            panic!("DatabaseConnection is not yet connected!");
        }

        self.client.as_mut().unwrap()
    }

    /// Connect to the database
    ///
    /// Non-TLS connections only supported at present.
    pub fn connect(&mut self) -> Result<(), String> {
        match pg::Client::connect(self.dsn(), pg::NoTls) {
            Ok(c) => {
                self.client = Some(c);
                Ok(())
            }
            Err(e) => Err(format!("Error connecting to database: {e}")),
        }
    }

    pub fn disconnect(&mut self) {
        self.client = None;
    }
}
