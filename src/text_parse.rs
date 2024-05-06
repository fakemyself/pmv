use prometheus::proto::{Metric, MetricFamily, MetricType};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::{self, Read};
use std::num::ParseFloatError;
use std::str;

#[derive(Debug)]
struct ParseError {
    msg: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "parse error: {}", self.msg)
    }
}

impl Error for ParseError {
    fn description(&self) -> &str {
        &self.msg
    }
}

#[derive(Debug)]
pub struct TextParser<R: Read> {
    current_byte: u8,

    current_labels: HashMap<String, String>,
    mf_by_name: HashMap<String, MetricFamily>,
    cur_mf_name: String,
    cur_mf_type: MetricType,

    current_token: Vec<u8>,
    current_bucket: f64,
    current_quantile: f64,
    current_is_summary_count: bool,
    current_is_summary_sum: bool,
    current_is_histogram_count: bool,
    current_is_histogram_sum: bool,
    line_count: i32,
    reading_bytes: i32,
    reader: R,

    cur_metrics: Vec<Metric>,

    error: Option<Box<dyn Error>>,
    state_fn: StateFn<R>,
}

type StateFn<R> = fn(&mut TextParser<R>) -> ParserState<R>;

enum ParserState<R: Read> {
    _Any(StateFn<R>),
    End,
}

impl<R: Read> TextParser<R> {
    pub fn new(reader: R) -> Self {
        TextParser {
            current_labels: HashMap::new(),
            mf_by_name: HashMap::new(),
            cur_mf_name: String::new(),
            cur_mf_type: MetricType::UNTYPED,
            cur_metrics: Vec::new(),

            current_token: Vec::new(),
            current_byte: 0 as u8,
            current_bucket: 0.0,
            current_quantile: 0.0,
            current_is_summary_count: false,
            current_is_summary_sum: false,
            current_is_histogram_count: false,
            current_is_histogram_sum: false,
            line_count: 0,
            reading_bytes: 0,
            reader: reader,
            error: None,
            state_fn: TextParser::start_of_line,
        }
    }

    pub fn text_to_metric_families(&mut self) -> Result<HashMap<String, MetricFamily>, io::Error> {
        loop {
            match (self.state_fn)(self) {
                ParserState::_Any(next) => {
                    self.state_fn = next;
                }
                ParserState::End => {
                    break;
                }
            }
        }

        Ok(HashMap::new()) // TODO: return empty
    }

    fn start_of_line(&mut self) -> ParserState<R> {
        println!("in start_of_line");

        self.line_count += 1;
        self.skip_blank_tab();

        match self.current_byte as char {
            '#' => self.start_comment(),

            '\n' => self.start_of_line(),

            _ => self.reading_metric_name(),
        }
    }

    fn start_comment(&mut self) -> ParserState<R> {
        println!("in start_comment");

        self.skip_blank_tab();
        if let Some(_err) = &self.error {
            return ParserState::End;
        }

        if self.current_byte == '\n' as u8 {
            return self.start_of_line();
        }

        self.read_token_until_white_space();
        if let Some(_err) = &self.error {
            return ParserState::End; // unexpected end of input.
        }

        if self.current_byte == '\n' as u8 {
            return self.start_of_line();
        }

        let mut on_help = false;
        let mut on_type = false;

        match str::from_utf8(&self.current_token) {
            Ok("HELP") => {
                on_help = true;
            }
            Ok("TYPE") => {
                on_type = true;
            }
            Ok(_) => {
                loop {
                    if self.current_byte == '\n' as u8 {
                        break;
                    }

                    if self.got_error() {
                        return ParserState::End;
                    }

                    self.read_byte()
                }
                return self.start_of_line();
            }
            Err(e) => {
                todo!("invalid UTF8 token: {}", e);
            }
        }

        // there is something. Next has to be a metric name.
        self.skip_blank_tab();
        if self.got_error() {
            return ParserState::End;
        }

        self.read_token_as_metric_name();
        if self.got_error() {
            return ParserState::End;
        }

        if self.current_byte == '\n' as u8 {
            return self.start_of_line();
        }

        if !is_blank_or_tab(self.current_byte) {
            return ParserState::End;
        }

        self.set_or_create_current_mf();

        self.skip_blank_tab();
        if self.got_error() {
            return ParserState::End;
        }

        if self.current_byte == '\n' as u8 {
            return self.start_of_line();
        }

        if on_help {
            return self.reading_help();
        }

        if on_type {
            return self.reading_type();
        }

        self.error = Some(Box::new(ParseError {
            msg: format!("code error: unexpected keyword"),
        }));

        ParserState::End
    }

    fn reading_help(&mut self) -> ParserState<R> {
        println!("in reading_help");

        self.read_token_until_newline(true);
        if self.got_error() {
            return ParserState::End;
        }

        if let Some(mf) = self.mf_by_name.get_mut(&self.cur_mf_name) {
            println!("get mf for {}", self.cur_mf_name);

            if mf.get_help().len() > 0 {
                self.error = Some(Box::new(ParseError {
                    msg: format!("second HELP line for metric name {}", mf.get_name()),
                }));
                return ParserState::End;
            }

            match String::from_utf8(self.current_token.clone()) {
                Ok(s) => {
                    mf.set_help(s);
                }
                Err(e) => {
                    self.error = Some(Box::new(e));
                }
            };
        } else {
            println!("mf {} not found", self.cur_mf_name);
        }

        println!("mf_by_name(after set HELP): {:?}", self.mf_by_name);

        self.start_of_line()
    }

    fn reading_type(&mut self) -> ParserState<R> {
        println!("in reading_type");

        self.read_token_until_newline(false);

        if self.got_error() {
            return ParserState::End;
        }

        println!("get TYPE {}", str::from_utf8(&self.current_token).unwrap());

        match str::from_utf8(&self.current_token) {
            Ok("summary") => {
                self.cur_mf_type = MetricType::SUMMARY;
            }
            Ok("counter") => {
                self.cur_mf_type = MetricType::COUNTER;
            }
            Ok("gauge") => {
                self.cur_mf_type = MetricType::GAUGE;
            }
            Ok("histogram") => {
                self.cur_mf_type = MetricType::HISTOGRAM;
            }
            _ => {
                self.cur_mf_type = MetricType::UNTYPED;
            }
        }

        if let Some(mf) = self.mf_by_name.get_mut(&self.cur_mf_name) {
            mf.set_field_type(self.cur_mf_type);
        }

        self.start_of_line()
    }

    fn set_or_create_current_mf(&mut self) {
        self.current_is_summary_count = false;
        self.current_is_summary_sum = false;
        self.current_is_histogram_count = false;
        self.current_is_histogram_sum = false;

        let name;

        match String::from_utf8(self.current_token.clone()) {
            Ok(s) => {
                name = s;
                println!("got name: {}", name);

                if self.mf_by_name.contains_key(&name) {
                    // key exist
                    return;
                }

                let sum_name = summary_metric_name(&name);
                match self.mf_by_name.get(sum_name) {
                    Some(mf) => {
                        self.cur_mf_name = sum_name.to_string();

                        if mf.get_field_type() == MetricType::SUMMARY {
                            if is_count(&name) {
                                self.current_is_summary_count = true;
                            }

                            if is_sum(&name) {
                                self.current_is_summary_sum = true;
                            }
                            return;
                        }
                    }
                    _ => {}
                }

                let histogram_name = histogram_metric_name(&name);
                match self.mf_by_name.get(histogram_name) {
                    Some(mf) => {
                        self.cur_mf_name = histogram_name.to_string();
                        if mf.get_field_type() == MetricType::HISTOGRAM {
                            if is_count(&name) {
                                self.current_is_histogram_count = true
                            }

                            if is_sum(&name) {
                                self.current_is_histogram_sum = true
                            }
                            return;
                        }
                    }
                    _ => {}
                }

                println!("add metric {}", name);
                self.cur_mf_name = name.clone();

                let mut mf = MetricFamily::new();
                mf.set_name(name.clone());
                self.mf_by_name.insert(name, mf);

                println!("mf_by_name: {:?}", self.mf_by_name);
            }
            Err(err) => {
                self.error = Some(Box::new(err));
            }
        }
    }

    fn read_token_as_metric_name(&mut self) {
        self.current_token.clear();

        if !is_valid_metric_name_start(self.current_byte as char) {
            return;
        }

        loop {
            self.current_token.push(self.current_byte);
            self.read_byte();

            if let Some(_err) = &self.error {
                println!("got error: {:?}", self.error);
                break;
            }

            if !is_valid_label_name_continuation(self.current_byte as char) {
                println!("got char: {}", self.current_byte as char);
                break;
            }
        }

        println!(
            "in read_token_as_metric_name: {}",
            str::from_utf8(&self.current_token).unwrap()
        );
    }

    fn reading_metric_name(&mut self) -> ParserState<R> {
        println!("in reading_metric_name");
        self.read_token_as_metric_name();

        if self.got_error() {
            return ParserState::End;
        }

        if self.current_token.len() == 0 {
            self.error = Some(Box::new(ParseError {
                msg: "invalid metric name".to_string(),
            }));
        }

        self.set_or_create_current_mf();

        if let Some(_) = self.mf_by_name.get_mut(&self.cur_mf_name) {
            self.cur_metrics.push(Metric::new());
        }
        self.reading_labels()
    }

    fn reading_labels(&mut self) -> ParserState<R> {
        println!("in reading_labels");

        if self.cur_mf_type == MetricType::HISTOGRAM || self.cur_mf_type == MetricType::SUMMARY {
            self.current_labels.clear();
            self.current_labels
                .entry("__name__".to_string())
                .or_insert(self.cur_mf_name.clone());
            self.current_quantile = std::f64::NAN;
            self.current_bucket = std::f64::NAN;
        }

        if self.current_byte != '{' as u8 {
            return self.reading_value();
        }

        self.start_label_name()
    }

    fn reading_value(&mut self) -> ParserState<R> {
        println!("in reading_value");

        let mut last_metric = self.cur_metrics.pop();

        if let Some(mf) = self.mf_by_name.get_mut(&self.cur_mf_name) {
            if self.cur_mf_type == MetricType::SUMMARY {
            } else if self.cur_mf_type == MetricType::HISTOGRAM {
            } else {
                //mf.get_field_type()
            }
        }

        self.read_token_until_white_space();
        if self.got_error() {
            return ParserState::End;
        }

        match parse_float(str::from_utf8(&self.current_token).unwrap()) {
            Ok(float_val) => {
                println!("get float {}", float_val);
            }
            Err(err) => {
                self.error = Some(Box::new(err));
            }
        }

        self.start_timestamp()
    }

    fn start_timestamp(&mut self) -> ParserState<R> {
        self.start_of_line()
    }

    fn start_label_name(&mut self) -> ParserState<R> {
        self.skip_blank_tab();
        if self.got_error() {
            return ParserState::End;
        }

        if self.current_byte == '}' as u8 {
            self.skip_blank_tab();
            if self.got_error() {
                return ParserState::End;
            }
            return self.reading_value();
        }

        self.read_token_as_label_name();
        if self.got_error() {
            return ParserState::End;
        }

        if self.current_token.len() == 0 {
            self.error = Some(Box::new(ParseError {
                msg: format!("invalid label name for metric {}", self.cur_mf_name),
            }));
            return ParserState::End;
        }

        self.start_label_value();
        if self.got_error() {
            return ParserState::End;
        }

        ParserState::End
    }

    fn read_token_as_label_name(&mut self) -> ParserState<R> {
        todo!();
        ParserState::End
    }

    fn got_error(&self) -> bool {
        if let Some(_) = &self.error {
            return true;
        }
        return false;
    }

    fn start_label_value(&mut self) -> ParserState<R> {
        ParserState::End
    }

    fn read_token_until_white_space(&mut self) {
        println!("in read_token_until_white_space");
        self.current_token.clear();
        loop {
            if let Some(_err) = &self.error {
                break;
            }

            if is_blank_or_tab(self.current_byte) || self.current_byte == '\n' as u8 {
                break;
            }

            self.current_token.push(self.current_byte);
            self.read_byte();
        }

        println!(
            "current token {}",
            str::from_utf8(&self.current_token).unwrap()
        );
    }

    fn skip_blank_tab(&mut self) {
        loop {
            self.read_byte();

            if let Some(_err) = &self.error {
                return;
            }

            if !is_blank_or_tab(self.current_byte) {
                return;
            }
        }
    }

    fn read_byte(&mut self) {
        let mut buf = [0; 1];
        match self.reader.read_exact(&mut buf) {
            Ok(_) => {
                self.reading_bytes += 1;
                self.error = None; // clear error
                self.current_byte = buf[0];
            }
            Err(err) => {
                self.error = Some(Box::new(err));
            }
        }
    }

    fn read_token_until_newline(&mut self, recognize_escape_seq: bool) {
        self.current_token.clear();

        let mut escaped = false;
        loop {
            if let Some(_err) = &self.error {
                return;
            }

            //println!(
            //    "read_token_until_newline: {}",
            //    str::from_utf8(&self.current_token).unwrap()
            //);

            if recognize_escape_seq && escaped {
                match self.current_byte as char {
                    '\\' => {
                        self.current_token.push(self.current_byte);
                    }
                    'n' => {
                        self.current_token.push('\n' as u8);
                    }
                    _ => {
                        self.error = Some(Box::new(ParseError {
                            msg: format!("invalid escape sequence '{}'", self.current_byte),
                        }))
                    }
                }
            } else {
                match self.current_byte as char {
                    '\n' => {
                        return;
                    }
                    '\\' => {
                        escaped = true;
                    }
                    _ => {
                        self.current_token.push(self.current_byte);
                    }
                }
            }
            self.read_byte()
        }
    }
}

fn is_blank_or_tab(b: u8) -> bool {
    return b == (' ' as u8) || b == ('\t' as u8);
}

fn is_valid_label_name_start(b: char) -> bool {
    return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_';
}

fn is_valid_label_name_continuation(b: char) -> bool {
    return is_valid_label_name_start(b) || (b >= '0' && b <= '9');
}

fn is_valid_metric_name_start(b: char) -> bool {
    return is_valid_label_name_start(b) || b == ':';
}

fn _is_valid_metric_name_continuation(b: char) -> bool {
    return is_valid_label_name_continuation(b) || b == ':';
}

fn summary_metric_name(name: &str) -> &str {
    if is_count(name) {
        &name[0..name.len() - 6]
    } else if is_sum(name) {
        &name[0..name.len() - 4]
    } else if is_bucket(name) {
        &name[0..name.len() - 7]
    } else {
        name
    }
}

fn histogram_metric_name(name: &str) -> &str {
    if is_count(name) {
        &name[0..name.len() - 6]
    } else if is_sum(name) {
        &name[0..name.len() - 4]
    } else if is_bucket(name) {
        &name[0..name.len() - 7]
    } else {
        name
    }
}

fn is_count(name: &str) -> bool {
    return name.ends_with("_count");
}

fn is_sum(name: &str) -> bool {
    return name.ends_with("_sum");
}

fn is_bucket(name: &str) -> bool {
    return name.ends_with("_bucket");
}

fn parse_float(s: &str) -> Result<f64, ParseFloatError> {
    s.parse::<f64>()
}

#[cfg(test)]

mod tests {

    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn test_basic_parse() {
        let cursor = Cursor::new(
            String::from(
                r#"
# HELP http_request_total The total number of HTTP requests.
# TYPE http_request_total counter
http_request_total{path="/api/v1",method="POST"} 1027
http_request_total{path="/api/v1",method="GET"} 4711
# HELP http_request_duration_seconds Summary of HTTP request durations in seconds.
# TYPE http_request_duration_seconds summary
http_request_duration_seconds{quantile="0.5"} 0.123
http_request_duration_seconds{quantile="0.9"} 0.456
http_request_duration_seconds{quantile="0.99"} 0.789
http_request_duration_seconds_sum 15.678
http_request_duration_seconds_count 1000
"#,
            )
            .into_bytes(),
        );

        let mut parser = TextParser::new(BufReader::new(cursor));

        let _ = parser.text_to_metric_families();
        println!(
            "reading bytes: {}, lines: {}",
            parser.reading_bytes, parser.line_count
        );
    }
}
