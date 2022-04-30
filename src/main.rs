use std::io;

use rusqlite::{Connection, Statement};
use csv::Writer;

mod parser;

struct Session {
    conn: Connection,
}

impl Session {
    pub fn new() -> rusqlite::Result<Session> {
        let conn = Connection::open_in_memory()?;
        rusqlite::vtab::csvtab::load_module(&conn)?;

        Ok(Session {
            conn,
        })
    }

    pub fn load_table(&self, name: &str, path: &str) -> rusqlite::Result<()> {
        self.conn.execute(format!("CREATE VIRTUAL TABLE [{}] USING csv(header=true,filename='{}')", name, path).as_str(), [])?;
        Ok(())
    }

    fn prepare_and_load(&self, query: &str) -> rusqlite::Result<Statement<'_>> {
        match self.conn.prepare(query) {
            Err(rusqlite::Error::SqliteFailure(libsqlite3_sys::Error{code: libsqlite3_sys::ErrorCode::Unknown, extended_code: 1}, Some(msg))) if msg.starts_with("no such table: ") => {
                let path = &msg["no such table: ".len()..];
                self.load_table(path, path)?;
                self.prepare_and_load(query)
            }
            x => x,
        }
    }

    pub fn execute<F>(&self, query: &str, on_record: &mut F) -> rusqlite::Result<()>
    where F: FnMut(Vec<String>) -> rusqlite::Result<()> {
        let mut stmt = self.prepare_and_load(query)?;

        let mut cols: Vec<String> = Vec::new();
        for c in stmt.column_names() {
            cols.push(c.to_string());
        }
        on_record(cols)?;
        let n = stmt.column_count();

        let iter = stmt.query_map([], |row| -> rusqlite::Result<Vec<String>> {
            Ok((0..n).map(|i| row.get_unwrap(i)).collect())
        })?;

        for row in iter {
            on_record(row?)?;
        }

        Ok(())
    }
}

fn main() -> rusqlite::Result<()> {
    let mut writer = Writer::from_writer(io::stdout());

    let sess = Session::new()?;
    sess.execute("SELECT * from [./test.csv]", &mut |row| {
        writer.write_record(row)?;
        Ok(())
    })?;

    let inp = "select [ab c] as a, def as d from [g hi], ./jkl.csv as j where max(abc) >= 5 group by mno, pqr having mno > 0";
    if let Ok(query) = parser::parse(inp) {
        println!("{}", query);
    }

    Ok(())
}
