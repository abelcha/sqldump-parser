use anyhow::Result;
use clap::Parser;
use csv::Writer;
use fs_more::directory::{move_directory, DirectoryMoveOptions};
use regex::Regex;
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::{GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use sqlparser::parser::Parser as SQLParser;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::{collections::HashMap, process::exit};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input SQL dump file
    #[arg(short, long)]
    input: PathBuf,

    /// Output directory for CSV files
    #[arg(short, long)]
    output_dir: PathBuf,

    /// Output directory for CSV files
    #[arg(short, long, default_value = "./")]
    dest: PathBuf,

    /// SQL dialect (mysql, postgres, sqlite)
    #[arg(short, long, default_value = "mysql")]
    dialect: String,
}

#[derive(Debug)]
struct Table {
    writer: Writer<File>,
}
static TMP_FILE: &str = "/tmp";

fn tmp_output_dir() -> PathBuf {
    let output_dir = Path::new(TMP_FILE).join(format!(".{}", std::process::id()));
    // fs::create_dir_all(&output_dir).unwrap();
    output_dir
}

impl Table {
    fn new(name: String, columns: Vec<String>) -> Self {
        // let output_dir = /tmp + pid

        let file_path = tmp_output_dir().join(format!("{}.csv", name.trim_matches('`')));
        println!("file_path: {:?}", file_path);
        let mut writer = Writer::from_path(file_path).unwrap();
        let clean_headers: Vec<String> = columns
            .iter()
            .map(|col| col.trim_matches('`').to_string())
            .collect();
        // println!("{}", clean_headers.join(","));
        writer.write_record(clean_headers).unwrap();
        println!("Create {:?}", &name);
        Self { writer }
    }

    fn add_row(&mut self, row: Vec<String>) {
        self.writer.write_record(row).unwrap();
    }
    fn close_writer(&mut self) {
        // self.writer..
        self.writer.flush().unwrap();
    }
}

fn get_dialect(name: &str) -> Box<dyn sqlparser::dialect::Dialect> {
    match name.to_lowercase().as_str() {
        "mysql" => Box::new(MySqlDialect {}),
        "postgres" => Box::new(PostgreSqlDialect {}),
        "sqlite" => Box::new(SQLiteDialect {}),
        _ => Box::new(GenericDialect {}),
    }
}

fn clean_sql_value(value: &str) -> String {
    let cleaned = value
        .trim_matches('\'')
        .trim_matches('"')
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t");

    if cleaned.eq_ignore_ascii_case("null") {
        String::new()
    } else {
        cleaned
    }
}

fn process_statement(
    sql: &str,
    dialect: &dyn sqlparser::dialect::Dialect,
    tables: &mut HashMap<String, Table>,
) -> Result<()> {
    // Remove TYPE=MyISAM from the end of CREATE TABLE statements
    let sql = sql.replace(" TYPE=MyISAM", "");

    if let Ok(ast) = SQLParser::parse_sql(dialect, sql.as_str()) {
        for stmt in ast {
            match stmt {
                Statement::CreateTable { name, columns, .. } => {
                    if (1 + tables.values().len()) % 50 == 0 {
                        println!("Flushing table");
                        tables.values_mut().for_each(|table| table.close_writer());
                        tables.clear();
                    }
                    let table_name = name.to_string().trim_matches('`').to_string();
                    println!("creating table name: {:?}", table_name);
                    let column_names = columns.iter().map(|c| c.name.to_string()).collect();
                    tables.insert(table_name.clone(), Table::new(table_name, column_names));
                }
                // Handle both INSERT and REPLACE statements
                Statement::Insert {
                    table_name,
                    source,
                    into,
                    columns,
                    ..
                } => {
                    // table_name.to_string();
                    let table_id = table_name.to_string().trim_matches('`').to_string();
                    println!("table_id: {:?}", table_id);
                    let mut table = tables.get_mut(&table_id.to_string());
                    if table.is_none() {
                        println!("table not found {:?}", table_id);
                        let column_names = columns
                            .iter()
                            .map(|c| c.value.to_string())
                            .collect::<Vec<String>>();
                        println!("creating table name: {} {:?}", table_id, column_names);
                        tables.insert(table_id.clone(), Table::new(table_id.clone(), column_names));
                        table = tables.get_mut(&table_id.clone().to_string());
                        // return Ok(());
                    }
                    let table = table.unwrap();

                    if let Some(query) = source.as_ref() {
                        match query.body.as_ref() {
                            SetExpr::Values(values) => {
                                for row in &values.rows {
                                    let row_data: Vec<String> = row
                                        .iter()
                                        .map(|v| clean_sql_value(&v.to_string()))
                                        .collect();
                                    table.add_row(row_data);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    fs::create_dir_all(tmp_output_dir())?;
    let file_name = args.input.file_name().unwrap().to_str().unwrap();
    let formatted_file_name = format!("{}-output", file_name);
    let destname = Path::new(&formatted_file_name)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    // Parse SQL dump
    let dialect = get_dialect(&args.dialect);
    println!("dialect {:?}", &args.dialect);
    // grepCountCreateTable.
    // parse_sql_dump(&args.input, dialect.as_ref()).context("Failed to parse SQL dump")?;
    let mut tables = HashMap::new();
    let file = File::open(args.input)?;
    let reader = BufReader::new(file);
    let mut current_sql = String::new();

    // Regex to match the start of SQL statements, including additional variants
    let statement_start = Regex::new(r"(?i)^(CREATE TABLE|INSERT( IGNORE)?( INTO)?|REPLACE INTO)")?;
    println!("outputdir: {:?}", tmp_output_dir());

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();

        // Skip comments and empty linesc
        if trimmed.starts_with("--") || trimmed.starts_with("/*") || trimmed.is_empty() {
            continue;
        }

        // If we find a new statement, process the previous one
        if statement_start.is_match(trimmed) && !current_sql.is_empty() {
            process_statement(&current_sql, dialect.as_ref(), &mut tables)?;
            current_sql.clear();
        }

        current_sql.push_str(&line);
        current_sql.push('\n');

        // Process statement if it ends with a semicolon
        if trimmed.ends_with(';') {
            process_statement(&current_sql, dialect.as_ref(), &mut tables)?;
            current_sql.clear();
        }
    }

    // Process any remaining SQL
    if !current_sql.is_empty() {
        process_statement(&current_sql, dialect.as_ref(), &mut tables)?;
    }
    // close all writers
    for table in tables.values_mut() {
        table.close_writer();
    }
    // id outpl allready exists:
    println!("Renaming {:?} to {:?}", tmp_output_dir(), &destname);
    let destpath = args.output_dir.join(&formatted_file_name);
    if fs::metadata(&destpath).is_ok() {
        println!("Removing {:?}", destpath);
        // let timestamp = std::time::SystemTime::now()
        //     .duration_since(std::time::UNIX_EPOCH)
        //     .unwrap()
        //     .as_secs();
        fs::remove_dir_all(&destpath)?;
        // fs::rename(
        //     &destpath,
        //     Path::new(TMP_FILE).join(format!("{}_{}", destname.to_string(), timestamp)),
        // )?;
    }
    println!("fs:Renaming {:?} to {:?}", tmp_output_dir(), &destpath);
    move_directory(tmp_output_dir(), &destpath, DirectoryMoveOptions::default())?;

    println!("Successfully created {:?}", destpath);
    Ok(())
}
