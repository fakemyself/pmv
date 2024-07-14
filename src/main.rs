use std::error::Error;

mod text_parse;

use env_logger::Builder;
use log::debug;
use log::LevelFilter;
use std::io::Write;
use text_parse::TextParser;

fn main() -> Result<(), Box<dyn Error>> {
    setup_logger();

    let r = std::fs::File::open("example.txt").expect("Fail to open file");
    let mut parser = TextParser::new(r);

    match parser.text_to_metric_families() {
        Ok(_) => parser.pretty_metrics(),
        Err(_) => {}
    }
    Ok(())
}

fn setup_logger() {
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {}:{} - {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .filter(None, LevelFilter::Debug)
        .init();
    debug!("logger setup ok");
}
