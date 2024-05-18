use chrono;
use log::{debug, error, LevelFilter};
use prometheus::proto::{Counter, LabelPair, Metric, MetricFamily, MetricType, Quantile, Summary};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::Write;
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
    cur_byte: u8,

    cur_labels: HashMap<String, String>,
    mf_by_name: HashMap<String, MetricFamily>,
    cur_mf_name: String,
    cur_mf_type: MetricType,

    cur_token: Vec<u8>,
    cur_bucket: f64,
    cur_quantile: f64,

    parser_status: Option<ParserStatus>,

    line_count: i32,
    reading_bytes: i32,
    reader: R,

    cur_metrics: Vec<Metric>,

    cur_label_pair: Option<LabelPair>,

    error: Option<Box<dyn Error>>,
    next_fn: Option<StateFn<R>>,
}

type StateFn<R> = fn(&mut TextParser<R>);

#[derive(Debug)]
enum ParserStatus {
    OnSummaryCount,
    OnSummarySum,
    OnHistogramCount,
    OnHistogramSum,
}

impl<'a, R: Read> TextParser<R> {
    pub fn new(reader: R) -> Self {
        TextParser {
            cur_labels: HashMap::new(),
            mf_by_name: HashMap::new(),
            cur_mf_name: String::new(),
            cur_mf_type: MetricType::UNTYPED,
            cur_metrics: Vec::new(),

            cur_token: Vec::new(),
            cur_byte: 0 as u8,
            cur_bucket: 0.0,
            cur_quantile: 0.0,

            parser_status: None,

            line_count: 0,
            reading_bytes: 0,
            reader: reader,
            error: None,
            cur_label_pair: None,
            next_fn: None,
        }
    }

    pub fn text_to_metric_families(&mut self) -> Result<HashMap<String, MetricFamily>, io::Error> {
        self.next_fn = Some(TextParser::start_of_line);
        loop {
            match self.next_fn {
                Some(next) => {
                    next(self);
                }
                None => {
                    debug!("on exit");
                    match &self.error {
                        Some(err) => {
                            error!("got error: {:?}", self.error);
                        }
                        None => {}
                    }
                    break;
                }
            }
        }

        Ok(HashMap::new()) // TODO: return empty
    }

    fn start_of_line(&mut self) {
        debug!("in start_of_line");

        self.line_count += 1;
        self.skip_blank_tab();

        match self.cur_byte as char {
            '#' => {
                self.next_fn = Some(TextParser::start_comment);
            }

            '\n' => {
                self.next_fn = Some(TextParser::start_of_line);
            }

            _ => {
                self.next_fn = Some(TextParser::reading_metric_name);
            }
        }
    }

    fn start_comment(&mut self) {
        debug!("in start_comment");

        self.skip_blank_tab();
        if let Some(_err) = &self.error {
            self.next_fn = None;
            return;
        }

        if self.cur_byte == '\n' as u8 {
            self.next_fn = Some(TextParser::start_of_line);
            return;
        }

        self.read_token_until_white_space();
        if let Some(_err) = &self.error {
            self.next_fn = None;
            return;
        }

        if self.cur_byte == '\n' as u8 {
            self.next_fn = Some(TextParser::start_of_line);
            return;
        }

        let mut on_help = false;
        let mut on_type = false;

        match str::from_utf8(&self.cur_token) {
            Ok("HELP") => {
                on_help = true;
            }
            Ok("TYPE") => {
                on_type = true;
            }
            Ok(_) => {
                loop {
                    if self.cur_byte == '\n' as u8 {
                        break;
                    }

                    if self.got_error() {
                        self.next_fn = None;
                        return;
                    }

                    self.read_byte()
                }
                self.next_fn = Some(TextParser::start_of_line);
                return;
            }

            Err(e) => {
                todo!("invalid UTF8 token: {}", e);
            }
        }

        // there is something. Next has to be a metric name.
        self.skip_blank_tab();
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        self.read_token_as_metric_name();
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        if self.cur_byte == '\n' as u8 {
            self.next_fn = Some(TextParser::start_of_line);
            return;
        }

        if !is_blank_or_tab(self.cur_byte) {
            self.next_fn = None;
            return;
        }

        self.set_or_create_cur_mf();

        self.skip_blank_tab();
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        if self.cur_byte == '\n' as u8 {
            self.next_fn = Some(TextParser::start_of_line);
            return;
        }

        if on_help {
            self.next_fn = Some(TextParser::reading_help);
            return;
        }

        if on_type {
            self.next_fn = Some(TextParser::reading_type);
            return;
        }

        self.error = Some(Box::new(ParseError {
            msg: format!("code error: unexpected keyword"),
        }));

        self.next_fn = None;
        return;
    }

    fn reading_help(&mut self) {
        debug!("in reading_help");

        self.read_token_until_newline(true);
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        if let Some(mf) = self.mf_by_name.get_mut(&self.cur_mf_name) {
            debug!("get mf for {}", self.cur_mf_name);

            if mf.get_help().len() > 0 {
                self.error = Some(Box::new(ParseError {
                    msg: format!("second HELP line for metric name {}", mf.get_name()),
                }));
                self.next_fn = None;
                return;
            }

            match String::from_utf8(self.cur_token.clone()) {
                Ok(s) => {
                    mf.set_help(s);
                }
                Err(e) => {
                    self.error = Some(Box::new(e));
                }
            };
        } else {
            debug!("mf {} not found", self.cur_mf_name);
        }

        debug!("mf_by_name(after set HELP): {:?}", self.mf_by_name);

        self.next_fn = Some(TextParser::start_of_line);
        return;
    }

    fn reading_type(&mut self) {
        debug!("in reading_type");

        self.read_token_until_newline(false);

        if self.got_error() {
            self.next_fn = None;
            return;
        }

        debug!("get TYPE {}", str::from_utf8(&self.cur_token).unwrap());

        match str::from_utf8(&self.cur_token) {
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

        self.next_fn = Some(TextParser::start_of_line);
        return;
    }

    fn set_or_create_cur_mf(&mut self) {
        self.parser_status = None;

        match String::from_utf8(self.cur_token.clone()) {
            Ok(name) => {
                debug!("got name: {}", name);

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
                                self.parser_status = Some(ParserStatus::OnSummaryCount);
                            }

                            if is_sum(&name) {
                                self.parser_status = Some(ParserStatus::OnSummarySum);
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
                                self.parser_status = Some(ParserStatus::OnHistogramCount);
                            }

                            if is_sum(&name) {
                                self.parser_status = Some(ParserStatus::OnHistogramSum);
                            }
                            return;
                        }
                    }
                    _ => {}
                }

                debug!("add metric {}", name);
                self.cur_mf_name = name.clone();

                let mut mf = MetricFamily::new();
                mf.set_name(name.clone());
                self.mf_by_name.insert(name, mf);

                debug!("mf_by_name: {:?}", self.mf_by_name);
            }
            Err(err) => {
                self.error = Some(Box::new(err));
            }
        }
    }

    fn read_token_as_metric_name(&mut self) {
        self.cur_token.clear();

        if !is_valid_metric_name_start(self.cur_byte as char) {
            return;
        }

        loop {
            self.cur_token.push(self.cur_byte);
            self.read_byte();

            if let Some(_err) = &self.error {
                debug!("got error: {:?}", self.error);
                break;
            }

            if !is_valid_label_name_continuation(self.cur_byte as char) {
                debug!("got char: {}", self.cur_byte as char);
                break;
            }
        }

        debug!(
            "in read_token_as_metric_name: {}",
            str::from_utf8(&self.cur_token).unwrap()
        );
    }

    fn reading_metric_name(&mut self) {
        debug!("in reading_metric_name");
        self.read_token_as_metric_name();

        if self.got_error() {
            self.next_fn = None;
            return;
        }

        if self.cur_token.len() == 0 {
            self.error = Some(Box::new(ParseError {
                msg: "invalid metric name".to_string(),
            }));
            self.next_fn = None;
            return;
        }

        self.set_or_create_cur_mf();

        if let Some(_) = self.mf_by_name.get_mut(&self.cur_mf_name) {
            self.cur_metrics.push(Metric::new());
        }

        self.next_fn = Some(TextParser::reading_labels);
        return;
    }

    fn reading_labels(&mut self) {
        debug!("in reading_labels");

        if self.cur_mf_type == MetricType::HISTOGRAM || self.cur_mf_type == MetricType::SUMMARY {
            self.cur_labels.clear();
            self.cur_labels
                .entry("__name__".to_string())
                .or_insert(self.cur_mf_name.clone());
            self.cur_quantile = std::f64::NAN;
            self.cur_bucket = std::f64::NAN;
        }

        if self.cur_byte != '{' as u8 {
            self.next_fn = Some(TextParser::reading_value);
            return;
        }

        self.next_fn = Some(TextParser::start_label_name);
        return;
    }

    fn reading_value(&mut self) {
        debug!("in reading_value");

        if let Some(last_metric) = self.cur_metrics.last() {
            if let Some(mf) = self.mf_by_name.get_mut(&self.cur_mf_name) {
                if self.cur_mf_type == MetricType::SUMMARY {
                } else if self.cur_mf_type == MetricType::HISTOGRAM {
                } else {
                    //mf.get_field_type()
                }
            }
        }

        self.read_token_until_white_space();
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        let float_val: f64 = 0.0;

        match parse_float(str::from_utf8(&self.cur_token).unwrap()) {
            Ok(float_val) => {
                debug!("get float {}", float_val);
            }
            Err(err) => {
                self.error = Some(Box::new(err));
            }
        }

        if let Some(m) = self.cur_metrics.last_mut() {
            match self.cur_mf_type {
                MetricType::COUNTER => {
                    let mut cnt = Counter::new();
                    cnt.set_value(float_val);
                    m.set_counter(cnt);
                }
                MetricType::GAUGE => {
                    todo!()
                }

                MetricType::SUMMARY => {
                    let mut sum = Summary::new();

                    match self.parser_status {
                        Some(ParserStatus::OnSummaryCount) => {
                            sum.set_sample_count(float_val as u64);
                        }
                        Some(ParserStatus::OnSummarySum) => {
                            sum.set_sample_sum(float_val);
                        }
                        _ => {
                            //let mut quantile = sum.get_quantile();
                            let mut q = Quantile::new();
                            q.set_quantile(self.cur_quantile);

                            //quantile.push_back()

                            self.error = Some(Box::new(
                                    ParseError { msg: "expect parser status to be summary count or summary sum, got None or other invalid status".to_string(), }));
                            self.next_fn = None;
                            return;
                        }
                    }

                    debug!("sum: {:?}, status: {:?}", sum, self.parser_status);
                }
                _ => {
                    todo!();
                }
            }

            debug!("metric: {:?}", m)
        }

        match self.cur_mf_type {
            _ => {}
        }

        self.start_timestamp()
    }

    fn start_timestamp(&mut self) {
        todo!();
    }

    fn start_label_name(&mut self) {
        self.skip_blank_tab();
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        if self.cur_byte == '}' as u8 {
            self.skip_blank_tab();
            if self.got_error() {
                self.next_fn = None;
                return;
            }

            self.next_fn = Some(TextParser::reading_value);
            return;
        }

        self.read_token_as_label_name();
        if self.got_error() {
            error!("error after read_token_as_label_name");
            self.next_fn = None;
            return;
        }

        if self.cur_token.len() == 0 {
            self.error = Some(Box::new(ParseError {
                msg: format!("invalid label name for metric {}", self.cur_mf_name),
            }));
            self.next_fn = None;
            return;
        }

        let label_name = String::from_utf8(self.cur_token.clone()).unwrap();

        self.cur_label_pair = Some(LabelPair::new());

        self.cur_label_pair.as_mut().unwrap().set_name(label_name);

        debug!("got label: {:?}", self.cur_label_pair);

        if self.cur_label_pair.as_ref().unwrap().get_name() == "__name__" {
            self.error = Some(Box::new(ParseError {
                msg: format!("label name `__name__' is reserved"),
            }))
        }

        // Special for summary/histogram: Do not add 'quantile' and 'le' label to 'real' labels.
        if !(self.cur_mf_type == MetricType::SUMMARY
            && self.cur_label_pair.as_ref().unwrap().get_name() == "quantile")
            && !(self.cur_mf_type == MetricType::HISTOGRAM
                && self.cur_label_pair.as_ref().unwrap().get_name() == "le")
        {
            if let Some(m) = self.cur_metrics.last_mut() {
                debug!("add label pair: {:?}", self.cur_label_pair);

                if let Some(lp) = self.cur_label_pair.take() {
                    m.mut_label().push(lp.clone());
                }

                debug!(
                    "cur_metrics: {:?}, cur_label_pair: {:?}",
                    self.cur_metrics, self.cur_label_pair
                );
            }
        }

        self.skip_blank_tab_if_current_blank_tab();

        if self.cur_byte != ('=' as u8) {
            self.error = Some(Box::new(ParseError {
                msg: format!(
                    "expect '=' after label name, found {}",
                    self.cur_byte as char
                ),
            }));

            debug!("on error {:?}", self.error);
            self.next_fn = None;
            return;
        }

        // TODO: check duplicate label name.

        self.start_label_value()
    }

    fn read_token_as_label_name(&mut self) {
        self.cur_token.clear();
        if !is_valid_label_name_start(self.cur_byte as char) {
            return;
        }

        loop {
            self.cur_token.push(self.cur_byte);
            self.read_byte();

            //debug!(
            //    "cur_token: {}, cur_byte: {}",
            //    str::from_utf8(&self.cur_token).unwrap(),
            //    self.cur_byte as char
            //);

            if self.got_error() || !is_valid_label_name_continuation(self.cur_byte as char) {
                return;
            }
        }
    }

    fn got_error(&self) -> bool {
        if let Some(_) = &self.error {
            return true;
        }
        return false;
    }

    fn start_label_value(&mut self) {
        debug!("in start_label_value, cur_byte: {}", self.cur_byte as char);

        self.skip_blank_tab();
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        if self.cur_byte != '"' as u8 {
            self.error = Some(Box::new(ParseError {
                msg: format!(
                    "expect '\"' after start of label value, found {}",
                    self.cur_byte as char,
                ),
            }));
            self.next_fn = None;
            return;
        }

        self.read_token_as_label_value();
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        if let Some(m) = self.cur_metrics.last_mut() {
            if let Some(lp) = m.mut_label().last_mut() {
                lp.set_value(String::from_utf8(self.cur_token.clone()).unwrap());

                debug!("lp: {:?}", lp);

                if self.cur_mf_type == MetricType::SUMMARY
                    || self.cur_mf_type == MetricType::HISTOGRAM
                {
                    if lp.get_name() == "quantile" || lp.get_name() == "le" {
                        // check if quantile/le float ok
                        match parse_float(str::from_utf8(&self.cur_token).unwrap()) {
                            Err(e) => {
                                debug!("parse_float: {}", e);
                                self.error = Some(Box::new(ParseError {
                                    msg: format!(
                                        "expect float as value for quantile lable, got {}",
                                        lp.get_value(),
                                    ),
                                }));
                                self.next_fn = None;
                                return;
                            }
                            Ok(v) => {
                                debug!("get quantile {}", v);
                                self.cur_quantile = v;
                            }
                        }
                    }

                    self.cur_labels
                        .insert(lp.get_name().to_string(), lp.get_value().to_string());
                }
                debug!(
                    "cur_labels: {:?}, mf_by_name: {:?}, lable pair: {:?}",
                    self.cur_labels, self.mf_by_name, lp,
                );
            }
        }

        self.skip_blank_tab();
        if let Some(_err) = &self.error {
            self.next_fn = None;
            return;
        }

        match self.cur_byte as char {
            ',' => {
                self.next_fn = Some(TextParser::start_label_name);
            }
            '}' => {
                self.skip_blank_tab();

                if let Some(_err) = &self.error {
                    self.next_fn = None;
                    return;
                }
                self.next_fn = Some(TextParser::reading_value);
            }
            _ => {
                self.next_fn = None;
                self.error = Some(Box::new(ParseError {
                    msg: format!("unexpected end of label value"),
                }));
                return;
            }
        }
    }

    fn read_token_as_label_value(&mut self) {
        self.cur_token.clear();
        let mut escaped = false;

        loop {
            self.read_byte();
            if self.got_error() {
                return;
            }

            if escaped {
                match self.cur_byte as char {
                    '"' | '\\' => {
                        self.cur_token.push(self.cur_byte);
                    }

                    'n' => {
                        self.cur_token.push('\n' as u8);
                    }

                    _ => {
                        self.error = Some(Box::new(ParseError {
                            msg: format!("invalid escape sequence '{}'", self.cur_byte),
                        }));
                        return;
                    }
                }

                escaped = false;
                continue;
            }

            match self.cur_byte as char {
                '"' => {
                    return;
                }
                '\n' => {
                    self.error = Some(Box::new(ParseError {
                        msg: format!(
                            "label value {} contains unescaped new-line",
                            str::from_utf8(&self.cur_token).unwrap()
                        ),
                    }))
                }
                '\\' => {
                    escaped = true;
                }
                _ => {
                    self.cur_token.push(self.cur_byte);
                }
            }
        }
    }

    fn read_token_until_white_space(&mut self) {
        debug!("in read_token_until_white_space");
        self.cur_token.clear();
        loop {
            if let Some(_err) = &self.error {
                break;
            }

            if is_blank_or_tab(self.cur_byte) || self.cur_byte == '\n' as u8 {
                break;
            }

            self.cur_token.push(self.cur_byte);
            self.read_byte();
        }

        debug!("cur token {}", str::from_utf8(&self.cur_token).unwrap());
    }

    fn skip_blank_tab(&mut self) {
        loop {
            self.read_byte();

            if let Some(_err) = &self.error {
                return;
            }

            if !is_blank_or_tab(self.cur_byte) {
                return;
            }
        }
    }

    fn skip_blank_tab_if_current_blank_tab(&mut self) {
        if is_blank_or_tab(self.cur_byte) {
            self.skip_blank_tab();
        }
    }

    fn read_byte(&mut self) {
        let mut buf = [0; 1];
        match self.reader.read_exact(&mut buf) {
            Ok(_) => {
                self.reading_bytes += 1;
                self.error = None; // clear error
                self.cur_byte = buf[0];
            }
            Err(err) => {
                self.error = Some(Box::new(err));
            }
        }
    }

    fn read_token_until_newline(&mut self, recognize_escape_seq: bool) {
        self.cur_token.clear();

        let mut escaped = false;
        loop {
            if let Some(_err) = &self.error {
                return;
            }

            //debug!(
            //    "read_token_until_newline: {}",
            //    str::from_utf8(&self.cur_token).unwrap()
            //);

            if recognize_escape_seq && escaped {
                match self.cur_byte as char {
                    '\\' => {
                        self.cur_token.push(self.cur_byte);
                    }
                    'n' => {
                        self.cur_token.push('\n' as u8);
                    }
                    _ => {
                        self.error = Some(Box::new(ParseError {
                            msg: format!("invalid escape sequence '{}'", self.cur_byte),
                        }))
                    }
                }
            } else {
                match self.cur_byte as char {
                    '\n' => {
                        return;
                    }
                    '\\' => {
                        escaped = true;
                    }
                    _ => {
                        self.cur_token.push(self.cur_byte);
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
        env_logger::Builder::new()
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

        debug!("testing test_basic_parse");

        let cursor = Cursor::new(
            String::from(
                r#"
# HELP http_request_duration_seconds Summary of HTTP request durations in seconds.
# TYPE http_request_duration_seconds summary
http_request_duration_seconds{quantile="0.5"} 0.123
http_request_duration_seconds{quantile="0.9"} 0.456
http_request_duration_seconds{quantile="0.99"} 0.789
http_request_duration_seconds_sum 15.678
http_request_duration_seconds_count 1000
# HELP http_request_total The total number of HTTP requests.
# TYPE http_request_total counter
http_request_total{path="/api/v1",method="POST"} 1027
http_request_total{path="/api/v1",method="GET"} 4711
"#,
            )
            .into_bytes(),
        );

        let mut parser = TextParser::new(BufReader::new(cursor));

        let _ = parser.text_to_metric_families();
        debug!(
            "reading bytes: {}, lines: {}",
            parser.reading_bytes, parser.line_count
        );
    }
}
