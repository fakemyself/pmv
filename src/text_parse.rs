use log::{debug, error};
use prometheus::proto::{
    Bucket, Counter, Gauge, Histogram, LabelPair, Metric, MetricFamily, MetricType, Quantile,
    Summary,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::{self, Read};
use std::num::ParseFloatError;
use std::rc::Rc;
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

    mf_by_name: HashMap<String, Rc<RefCell<MetricFamily>>>,
    cur_mf: Rc<RefCell<MetricFamily>>,

    cur_token: Vec<u8>,
    cur_bucket: f64,
    cur_quantile: f64,

    parser_status: Option<ParserStatus>,

    line_count: i32,
    reading_bytes: i32,
    reader: R,

    cur_metric: Option<Metric>,
    cur_lp: Option<LabelPair>,

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
            cur_mf: Rc::new(RefCell::new(MetricFamily::new())),

            cur_metric: None,

            cur_token: Vec::new(),
            cur_byte: 0 as u8,
            cur_bucket: 0.0,
            cur_quantile: 0.0,

            parser_status: None,

            line_count: 0,
            reading_bytes: 0,
            reader: reader,
            error: None,
            cur_lp: None,
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
                    for (k, v) in self.mf_by_name.iter() {
                        debug!("=> {}: {:?}", k, v.borrow());
                    }
                    match &self.error {
                        Some(_err) => {
                            error!("got error: {:?}", _err);
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
        debug!("in start-of-line");

        self.line_count += 1;
        self.skip_blank_tab();
        if self.error.is_some() {
            return;
        }

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
        debug!("in start-comment");

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

                if self.next_fn.is_none() && self.error.is_some() {
                    todo!("EOF");
                } else {
                    self.next_fn = Some(TextParser::start_of_line);
                    return;
                }
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
        debug!("in reading-help");

        self.read_token_until_newline(true);
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        // On new help, we think there is a new metric family comming.
        self.cur_mf = Rc::new(RefCell::new(MetricFamily::new()));

        let mut mf = self.cur_mf.borrow_mut();

        debug!("get mf {:?}", mf);

        if mf.get_help().len() > 0 {
            self.error = Some(Box::new(ParseError {
                msg: format!(
                    "second HELP line for metric name {}, help: {}",
                    mf.get_name(),
                    mf.get_help()
                ),
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

        debug!("mf_by_name(after set HELP): {:?}", self.mf_by_name);

        self.next_fn = Some(TextParser::start_of_line);
        return;
    }

    fn reading_type(&mut self) {
        debug!("in reading-type");

        self.read_token_until_newline(false);

        if self.got_error() {
            self.next_fn = None;
            return;
        }

        debug!("get TYPE {}", str::from_utf8(&self.cur_token).unwrap());

        match str::from_utf8(&self.cur_token) {
            Ok("summary") => {
                self.cur_mf.borrow_mut().set_field_type(MetricType::SUMMARY);
            }
            Ok("counter") => {
                self.cur_mf.borrow_mut().set_field_type(MetricType::COUNTER);
            }
            Ok("gauge") => {
                self.cur_mf.borrow_mut().set_field_type(MetricType::GAUGE);
            }
            Ok("histogram") => {
                self.cur_mf
                    .borrow_mut()
                    .set_field_type(MetricType::HISTOGRAM);
            }
            _ => {
                todo!(
                    "token '{}' got unknown type",
                    str::from_utf8(&self.cur_token).unwrap()
                );
                //self.cur_mf.borrow_mut().set_field_type(MetricType::UNTYPED);
            }
        }

        self.next_fn = Some(TextParser::start_of_line);
        return;
    }

    fn set_or_create_cur_mf(&mut self) {
        self.parser_status = None;

        match String::from_utf8(self.cur_token.clone()) {
            Ok(name) => {
                debug!("got name: {}", name);

                if self.cur_mf.borrow().get_name() == name {
                    debug!("name {} exist, skipped", name);
                    return;
                }

                //if self.mf_by_name.contains_key(&name) {
                //    // key exist
                //    return;
                //}

                let sum_name = summary_metric_name(&name);
                let histogram_name = histogram_metric_name(&name);

                let mut mf = self.cur_mf.borrow_mut();
                if mf.get_name() == sum_name {
                    if mf.get_field_type() == MetricType::SUMMARY {
                        if is_count(&name) {
                            self.parser_status = Some(ParserStatus::OnSummaryCount);
                        } else if is_sum(&name) {
                            self.parser_status = Some(ParserStatus::OnSummarySum);
                        }
                        return;
                    }
                } else if mf.get_name() == histogram_name {
                    if mf.get_field_type() == MetricType::SUMMARY {
                        if is_count(&name) {
                            self.parser_status = Some(ParserStatus::OnHistogramCount);
                        } else if is_sum(&name) {
                            self.parser_status = Some(ParserStatus::OnHistogramSum);
                        }
                        return;
                    }
                }

                debug!("add metric {}", name);

                mf.set_name(name.clone());

                self.mf_by_name.insert(name, self.cur_mf.clone());

                debug!("mf-by-name: {:?}", self.mf_by_name);
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
            "------------------\nin read-token-as-metric-name: {}\n---------------------------",
            str::from_utf8(&self.cur_token).unwrap()
        );
    }

    fn reading_metric_name(&mut self) {
        debug!("in reading-metric-name");
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

        self.cur_metric = Some(Metric::new());

        self.skip_blank_tab_if_current_blank_tab();
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        self.next_fn = Some(TextParser::reading_labels);
        return;
    }

    fn reading_labels(&mut self) {
        debug!("in reading-labels");

        match self.cur_mf.borrow().get_field_type() {
            MetricType::HISTOGRAM | MetricType::SUMMARY => {
                self.cur_labels.clear();
                self.cur_labels
                    .entry("__name__".to_string())
                    .or_insert(self.cur_mf.borrow().get_name().to_string());
                self.cur_quantile = std::f64::NAN;
                self.cur_bucket = std::f64::NAN;

                debug!("cur_labels: {:?}", self.cur_labels);
            }
            _ => {}
        }

        if self.cur_byte != '{' as u8 {
            debug!(
                "got '{}', no label, directly reading value",
                self.cur_byte as char
            );
            self.next_fn = Some(TextParser::reading_value);
            return;
        }

        self.next_fn = Some(TextParser::start_label_name);
        return;
    }

    fn reading_value(&mut self) {
        debug!("in reading_value");

        self.read_token_until_white_space();
        if self.got_error() {
            self.next_fn = None;
            return;
        }

        let float_val: f64;
        match parse_float(str::from_utf8(&self.cur_token).unwrap()) {
            Ok(f) => {
                float_val = f;
                debug!("get float {}", float_val);
            }
            Err(err) => {
                error!("parse float: {}", err);
                self.error = Some(Box::new(err));
                self.next_fn = None;
                return;
            }
        }

        match self.cur_mf.borrow().get_field_type() {
            MetricType::COUNTER => {
                let mut cnt = Counter::new();
                cnt.set_value(float_val);
                self.cur_metric.as_mut().unwrap().set_counter(cnt);
                debug!("get counter: {:?}", self.cur_metric);
            }

            MetricType::GAUGE => {
                let mut gauge = Gauge::new();
                gauge.set_value(float_val);
                self.cur_metric.as_mut().unwrap().set_gauge(gauge);
                debug!("get gauge {:?}", self.cur_metric);
            }

            MetricType::HISTOGRAM => {
                if self.cur_metric.is_none() {
                    self.cur_metric
                        .as_mut()
                        .unwrap()
                        .set_histogram(Histogram::new());
                }

                match self.parser_status {
                    Some(ParserStatus::OnHistogramCount) => {
                        self.cur_metric
                            .as_mut()
                            .unwrap()
                            .mut_histogram()
                            .set_sample_count(float_val as u64);
                    }
                    Some(ParserStatus::OnHistogramSum) => {
                        self.cur_metric
                            .as_mut()
                            .unwrap()
                            .mut_histogram()
                            .set_sample_sum(float_val);
                    }
                    _ => {
                        if self.cur_bucket != std::f64::NAN {
                            let mut bkt = Bucket::new();
                            bkt.set_upper_bound(self.cur_bucket);
                            bkt.set_cumulative_count(float_val as u64);
                            debug!("set bucket: {:?}", bkt);
                            self.cur_metric
                                .as_mut()
                                .unwrap()
                                .mut_histogram()
                                .mut_bucket()
                                .push(bkt);
                        }
                    }
                }

                debug!(
                    "histo: {:?}, status: {:?}",
                    self.cur_metric.as_ref().unwrap().get_histogram(),
                    self.parser_status
                );
                debug!("cur_metric: {:?}", self.cur_metric);
            }

            MetricType::SUMMARY => {
                if self.cur_metric.is_none() {
                    self.cur_metric
                        .as_mut()
                        .unwrap()
                        .set_summary(Summary::new());
                }

                match self.parser_status {
                    Some(ParserStatus::OnSummaryCount) => {
                        debug!("set sample count: {}", float_val);
                        self.cur_metric
                            .as_mut()
                            .unwrap()
                            .mut_summary()
                            .set_sample_count(float_val as u64);
                    }
                    Some(ParserStatus::OnSummarySum) => {
                        debug!("set sample sum: {}", float_val);
                        self.cur_metric
                            .as_mut()
                            .unwrap()
                            .mut_summary()
                            .set_sample_sum(float_val);
                    }
                    _ => {
                        if self.cur_quantile != std::f64::NAN {
                            let mut q = Quantile::new();
                            q.set_quantile(self.cur_quantile);
                            self.cur_metric
                                .as_mut()
                                .unwrap()
                                .mut_summary()
                                .mut_quantile()
                                .push(q);
                            debug!("cur_metric: {:?}", self.cur_metric);
                        }
                    }
                }

                debug!(
                    "sum: {:?}, status: {:?}",
                    self.cur_metric.as_ref().unwrap().get_summary(),
                    self.parser_status
                );
            }
            MetricType::UNTYPED => {
                todo!("");
            }
        }

        debug!("cur_metric: {:?}", self.cur_metric.as_ref().unwrap());

        let ftype = self.cur_mf.borrow().get_field_type();

        match ftype {
            MetricType::SUMMARY => {
                debug!("we are summary");
                // TODO: append self.cur_metric to self.cur_mf
            }
            MetricType::HISTOGRAM => {
                debug!("we are histo");
                // TODO: append self.cur_metric to self.cur_mf
            }
            _ => {
                debug!(
                    "on {:?}, append cur-metric{:?} to self.cur_mf",
                    ftype, self.cur_metric
                );

                match &self.cur_metric {
                    None => {}
                    Some(m) => {
                        self.cur_mf.borrow_mut().mut_metric().push(m.clone());
                    }
                }
            }
        }

        if self.cur_byte == '\n' as u8 {
            self.next_fn = Some(Self::start_of_line);
            return;
        } else {
            debug!("cur_byte: {}", self.cur_byte);
            self.next_fn = Some(Self::start_timestamp);
            return;
        }
    }

    fn start_timestamp(&mut self) {
        debug!("self: {:?}", self.parser_status);
        todo!("TODO: self.start_timestamp");
        //self.skip_blank_tab();
        //if self.got_error() {
        //    self.next_fn = None;
        //    return;
        //}

        //self.read_token_until_white_space();
        //if self.got_error() {
        //    self.next_fn = None;
        //    return;
        //}
    }

    fn start_label_name(&mut self) {
        debug!("in start-label-name");

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
                msg: format!(
                    "invalid label name for metric {}",
                    self.cur_mf.borrow().get_name()
                ),
            }));
            self.next_fn = None;
            return;
        }

        let label_name = String::from_utf8(self.cur_token.clone()).unwrap();

        self.cur_lp = Some(LabelPair::new());

        self.cur_lp.as_mut().unwrap().set_name(label_name);

        debug!("got label-pair: {:?}", self.cur_lp);

        if self.cur_lp.as_ref().unwrap().get_name() == "__name__" {
            self.error = Some(Box::new(ParseError {
                msg: format!("label name `__name__' is reserved"),
            }))
        }

        // Special for summary/histogram: Do not add 'quantile' and 'le' label to 'real' labels.
        match self.cur_mf.borrow().get_field_type() {
            MetricType::SUMMARY | MetricType::HISTOGRAM => {
                // TODO: what if other label-key that not 'quantile' and 'le'?
            } // pass
            _ => {
                let lp_name = self.cur_lp.as_ref().unwrap().get_name();
                if lp_name != "le" && lp_name != "quantile" {
                    debug!("cur-label-pair '{:?}'", self.cur_lp);

                    self.cur_metric
                        .as_mut()
                        .unwrap()
                        .mut_label()
                        .push(self.cur_lp.take().unwrap());

                    debug!(
                        "cur-metric: {:?}, cur-label-pair: {:?}",
                        self.cur_metric, self.cur_lp
                    );
                }
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

        self.next_fn = Some(Self::start_label_value);
        return;
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
        debug!("in start-label-value, cur_byte: {}", self.cur_byte as char);

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

        // TODO: test if label value is valid.
        match self.cur_metric.as_mut().unwrap().mut_label().last_mut() {
            Some(cur_lp) => {
                cur_lp.set_value(String::from_utf8(self.cur_token.clone()).unwrap());
                debug!("cur-lp: {:?}", cur_lp);

                self.cur_labels.insert(
                    cur_lp.get_name().to_string(),
                    cur_lp.get_value().to_string(),
                );
            }
            None => {
                debug!(
                    "cur_lp not set for type {:?}",
                    self.cur_mf.borrow().get_field_type()
                );
            }
        }

        match self.cur_mf.borrow().get_field_type() {
            MetricType::SUMMARY => {
                if self.cur_lp.as_ref().unwrap().get_name() == "quantile" {
                    // check if quantile float ok
                    match parse_float(str::from_utf8(&self.cur_token).unwrap()) {
                        Err(e) => {
                            debug!("parse_float: {}", e);
                            self.error = Some(Box::new(ParseError {
                                msg: format!(
                                    "expect float as value for quantile lable, got {}",
                                    self.cur_lp.as_ref().unwrap().get_value(),
                                ),
                            }));
                            self.next_fn = None;
                            return;
                        }
                        Ok(v) => {
                            debug!("set cur_quantile: {}", v);
                            self.cur_quantile = v;
                        }
                    }
                }
            }

            MetricType::HISTOGRAM => {
                if self.cur_lp.as_ref().unwrap().get_name() == "le" {
                    // check if 'le' float ok
                    match parse_float(str::from_utf8(&self.cur_token).unwrap()) {
                        Err(e) => {
                            debug!("parse_float: {}", e);
                            self.error = Some(Box::new(ParseError {
                                msg: format!(
                                    "expect float as value for le lable, got {}",
                                    self.cur_lp.as_ref().unwrap().get_value(),
                                ),
                            }));
                            self.next_fn = None;
                            return;
                        }
                        Ok(v) => {
                            debug!("set cur_quantile: {}", v);
                            self.cur_bucket = v;
                        }
                    }
                }
            }

            _ => {}
        }

        debug!(
            "cur_labels: {:?}, mf_by_name: {:?}, lable pair: {:?}",
            self.cur_labels, self.mf_by_name, self.cur_lp,
        );

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
        debug!(
            "in read-token-until-white-space, cur_byte: '{}'",
            self.cur_byte as char
        );
        self.cur_token.clear();
        loop {
            if let Some(_err) = &self.error {
                break;
            }

            if !is_blank_or_tab(self.cur_byte) && self.cur_byte != '\n' as u8 {
                self.cur_token.push(self.cur_byte);
                self.read_byte();
            } else {
                debug!("got '{}'", self.cur_byte as char);
                break;
            }
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
                error!("read_exact: {:?}", err);
                self.error = Some(Box::new(err));
                self.next_fn = None
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
    use chrono;
    use log::LevelFilter;
    use std::io::Write;
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
# HELP http2_request_duration_seconds Histogram of HTTP request latencies in seconds.
# TYPE http2_request_duration_seconds histogram
http2_request_duration_seconds_bucket{api="/v1/write", le="0.1"} 100
http2_request_duration_seconds_bucket{api="/v1/write", le="0.2"} 250
http2_request_duration_seconds_bucket{api="/v1/write", le="0.5"} 500
http2_request_duration_seconds_bucket{api="/v1/write", le="1.0"} 700
http2_request_duration_seconds_bucket{api="/v1/write", le="+Inf"} 850
http2_request_duration_seconds_sum{api="/v1/write"} 52.3
http2_request_duration_seconds_count{api="/v1/write"} 850
# HELP some_counter Some counter.
# TYPE some_counter counter
some_counter{path="/api/v1",method="POST"} 1027
some_counter{path="/api/v1",method="GET"} 4711
# HELP some_gauge Some gauge.
# TYPE some_gauge gauge
some_gauge{path="/api/v1",method="POST"} 1028
some_gauge{path="/api/v1",method="GET"} 4712
# HELP api_latency_seconds HTTP request latency partitioned by HTTP API and HTTP status
# TYPE api_latency_seconds summary
api_latency_seconds{api="/v1/pull",status="url-error",quantile="0.5"} 0.000952746
api_latency_seconds{api="/v1/pull",status="url-error",quantile="0.9"} 0.00546789
api_latency_seconds{api="/v1/pull",status="url-error",quantile="0.99"} 0.009414857
api_latency_seconds_sum{api="/v1/pull",status="url-error"} 0.1711532899999999
api_latency_seconds_count{api="/v1/pull",status="url-error"} 104
api_latency_seconds{api="/v1/usage_trace",status="url-error",quantile="0.5"} 0.000855108
api_latency_seconds{api="/v1/usage_trace",status="url-error",quantile="0.9"} 0.001084062
api_latency_seconds{api="/v1/usage_trace",status="url-error",quantile="0.99"} 0.001084062
api_latency_seconds_sum{api="/v1/usage_trace",status="url-error"} 0.0032156570000000002
api_latency_seconds_count{api="/v1/usage_trace",status="url-error"} 4
api_latency_seconds{api="/v1/drop",status="url-error",quantile="0.5"} 0.000979155
api_latency_seconds{api="/v1/drop",status="url-error",quantile="0.9"} 0.006829465
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
