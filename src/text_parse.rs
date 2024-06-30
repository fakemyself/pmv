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
            next_fn: None,
        }
    }

    fn pretty_metrics(&self) {
        for (k, v) in self.mf_by_name.iter() {
            debug!(
                "=> {}: {}/{:?}: {}",
                k,
                v.borrow().get_name(),
                v.borrow().get_field_type(),
                v.borrow().get_help(),
            );

            for m in v.borrow().get_metric() {
                debug!("\t {:?}", m);
            }
        }
    }

    pub fn text_to_metric_families(&mut self) -> Result<(), io::Error> {
        self.next_fn = Some(TextParser::start_of_line);
        loop {
            match self.next_fn {
                Some(next) => next(self),
                None => match &self.error {
                    Some(_err) => {
                        error!("get error: {:?}", _err);
                        break;
                    }
                    None => {
                        break;
                    }
                },
            }
        }
        Ok(())

        //Ok(HashMap::new()) // TODO: return empty
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

        debug!("mf-by-name(after set HELP): {:?}", self.mf_by_name);

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
                debug!("get name: {}, cur-metric: {:?}", name, self.cur_metric);

                if self.cur_mf.borrow().get_name() == name {
                    debug!("name {} exist, skipped", name);
                    return;
                }

                if self.mf_by_name.contains_key(&name) {
                    // key exist: should we return here?
                    debug!("{} exist in mf-by-name: {:?}", name, self.mf_by_name);
                    return;
                }

                {
                    let mf = self.cur_mf.borrow();
                    let mf_type = mf.get_field_type();
                    debug!("name: {}, cur-mf name: {}", name, mf.get_name(),);

                    if mf_type == MetricType::SUMMARY {
                        if mf.get_name() == summary_metric_name(&name) {
                            if is_count(&name) {
                                self.parser_status = Some(ParserStatus::OnSummaryCount);
                            } else if is_sum(&name) {
                                self.parser_status = Some(ParserStatus::OnSummarySum);
                            }
                            return;
                        }
                    } else if mf_type == MetricType::HISTOGRAM {
                        if mf.get_name() == histogram_metric_name(&name) {
                            if is_count(&name) {
                                self.parser_status = Some(ParserStatus::OnHistogramCount);
                            } else if is_sum(&name) {
                                self.parser_status = Some(ParserStatus::OnHistogramSum);
                            }
                            return;
                        }
                    }
                }

                self.cur_mf = Rc::new(RefCell::new(MetricFamily::new()));
                self.cur_mf.borrow_mut().set_name(name.clone());
                self.mf_by_name.insert(name, self.cur_mf.clone());

                debug!(
                    "add metric {:?}, mf-by-name: {}",
                    self.cur_mf.borrow(),
                    self.mf_by_name.len(),
                );
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
                debug!("get error: {:?}", self.error);
                break;
            }

            if !is_valid_label_name_continuation(self.cur_byte as char) {
                debug!("get char: {}", self.cur_byte as char);
                break;
            }
        }

        debug!(
            "in read-token-as-metric-name: {}\n",
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

                debug!("cur-labels: {:?}", self.cur_labels);
            }
            _ => {}
        }

        if self.cur_byte != '{' as u8 {
            debug!(
                "get '{}', no label, directly reading value",
                self.cur_byte as char
            );
            self.next_fn = Some(TextParser::reading_value);
            return;
        }

        self.next_fn = Some(TextParser::start_label_name);

        debug!("cur-metric: {:?}", self.cur_metric);
        return;
    }

    fn reading_value(&mut self) {
        debug!("in reading-value, cur-metric: {:?}", self.cur_metric);

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

        let mftype = self.cur_mf.borrow().get_field_type();

        match mftype {
            MetricType::COUNTER => {
                let mut cnt = Counter::new();
                cnt.set_value(float_val);
                self.cur_metric.as_mut().unwrap().set_counter(cnt);
                debug!("get counter: {:?}", self.cur_metric);

                match &self.cur_metric {
                    None => {}
                    Some(m) => {
                        self.cur_mf.borrow_mut().mut_metric().push(m.clone());
                    }
                }
            }

            MetricType::GAUGE => {
                let mut gauge = Gauge::new();
                gauge.set_value(float_val);
                self.cur_metric.as_mut().unwrap().set_gauge(gauge);
                debug!("get gauge {:?}", self.cur_metric);

                match &self.cur_metric {
                    None => {}
                    Some(m) => {
                        self.cur_mf.borrow_mut().mut_metric().push(m.clone());
                    }
                }
            }

            MetricType::HISTOGRAM => {
                if self.cur_metric.is_none() {
                    self.cur_metric
                        .as_mut()
                        .unwrap()
                        .set_histogram(Histogram::new());
                }

                debug!("parser-status: {:?}", self.parser_status);

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
                debug!("get histo: {:?}", self.cur_metric);

                match &self.cur_metric {
                    None => {}
                    Some(m) => {
                        self.cur_mf.borrow_mut().mut_metric().push(m.clone());
                    }
                }
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
                            q.set_value(float_val);

                            debug!("set quantile: {:?}", q);

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
                    "get summary: {:?}, status: {:?}",
                    self.cur_metric.as_ref().unwrap().get_summary(),
                    self.parser_status
                );

                match &self.cur_metric {
                    None => {}
                    Some(m) => {
                        self.cur_mf.borrow_mut().mut_metric().push(m.clone());
                    }
                }
            }
            MetricType::UNTYPED => {
                todo!("");
            }
        }

        debug!("cur_metric: {:?}", self.cur_metric.as_ref().unwrap());
        // TODO: should we clear self.cur_metric?

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

        // Set metric type if there is no TYPE hint available.
        match label_name.as_str() {
            "le" => self
                .cur_mf
                .borrow_mut()
                .set_field_type(MetricType::HISTOGRAM),
            "quantile" => self.cur_mf.borrow_mut().set_field_type(MetricType::SUMMARY),
            _ => {} // pass
        }

        let mut cur_lp = LabelPair::new();

        cur_lp.set_name(label_name);

        debug!("get label-pair: {:?}", cur_lp);

        if cur_lp.get_name() == "__name__" {
            self.error = Some(Box::new(ParseError {
                msg: format!("label name `__name__' is reserved"),
            }))
        }

        self.cur_metric.as_mut().unwrap().mut_label().push(cur_lp);
        debug!("cur-metric: {:?}", self.cur_metric);

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

        debug!("cur-metric: {:?}", self.cur_metric);
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

        debug!("cur-metric: {:?}", self.cur_metric);

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
                    "cur-lp not set for type {:?}",
                    self.cur_mf.borrow().get_field_type()
                );
            }
        }

        match self.cur_mf.borrow().get_field_type() {
            MetricType::SUMMARY => {
                match self.cur_metric.as_mut().unwrap().mut_label().last_mut() {
                    Some(cur_lp) => {
                        debug!("cur-lp: {:?}", cur_lp);
                        if cur_lp.get_name() == "quantile" {
                            // check if quantile float ok
                            match parse_float(str::from_utf8(&self.cur_token).unwrap()) {
                                Err(e) => {
                                    debug!("parse_float: {}", e);
                                    self.error = Some(Box::new(ParseError {
                                        msg: format!(
                                            "expect float as value for quantile lable, got {}",
                                            cur_lp.get_value(),
                                        ),
                                    }));
                                    self.next_fn = None;
                                    return;
                                }
                                Ok(v) => {
                                    debug!("set cur-quantile: {}", v);
                                    self.cur_quantile = v;
                                }
                            }
                        }
                    }
                    None => {}
                }
            }

            MetricType::HISTOGRAM => {
                match self.cur_metric.as_mut().unwrap().mut_label().last_mut() {
                    Some(cur_lp) => {
                        debug!("cur-lp: {:?}", cur_lp);

                        if cur_lp.get_name() == "le" {
                            // check if 'le' float ok
                            match parse_float(str::from_utf8(&self.cur_token).unwrap()) {
                                Err(e) => {
                                    debug!("parse_float: {}", e);
                                    self.error = Some(Box::new(ParseError {
                                        msg: format!(
                                            "expect float as value for le lable, got {}",
                                            cur_lp.get_value(),
                                        ),
                                    }));
                                    self.next_fn = None;
                                    return;
                                }
                                Ok(v) => {
                                    debug!("set cur-bucket: {}", v);
                                    self.cur_bucket = v;
                                }
                            }
                        }
                    }
                    None => {}
                }
            }

            _ => {}
        }

        debug!(
            "cur-labels: {:?}, mf-by-name: {}",
            self.cur_labels,
            self.mf_by_name.len()
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
                debug!("get '{}'", self.cur_byte as char);
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
    fn setup_logger() {
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
        debug!("logger setup ok");
    }

    #[test]
    fn test_count_parse() {
        debug!("in test_count_parse");

        let cursor = Cursor::new(
            String::from(
                r#"
# HELP some_other_counter Some counter.
# TYPE some_other_counter counter
some_other_counter{path="/api/v1",method="POST"} 1027
some_other_counter{path="/api/v1",method="GET"} 4711
"#,
            )
            .into_bytes(),
        );

        let mut parser = TextParser::new(BufReader::new(cursor));
        let _ = parser.text_to_metric_families();
        parser.pretty_metrics();
        debug!(
            "reading bytes: {}, lines: {}",
            parser.reading_bytes, parser.line_count
        );
    }

    #[test]
    fn test_go_runtime_metrics() {
        //setup_logger();
        let mut parser = TextParser::new(BufReader::new(Cursor::new(
            String::from(
                r#"
go_cgo_go_to_c_calls_calls_total 8447
go_gc_cycles_automatic_gc_cycles_total 10
go_gc_cycles_forced_gc_cycles_total 0
go_gc_cycles_total_gc_cycles_total 10
go_gc_duration_seconds{quantile="0"} 3.4709e-05
go_gc_duration_seconds{quantile="0.25"} 3.9917e-05
go_gc_duration_seconds{quantile="0.5"} 0.000138459
go_gc_duration_seconds{quantile="0.75"} 0.000211333
go_gc_duration_seconds{quantile="1"} 0.000693833
go_gc_duration_seconds_sum 0.001920708
go_gc_duration_seconds_count 10
go_gc_heap_allocs_by_size_bytes_bucket{le="8.999999999999998"} 16889
go_gc_heap_allocs_by_size_bytes_bucket{le="24.999999999999996"} 221293
go_gc_heap_allocs_by_size_bytes_bucket{le="64.99999999999999"} 365672
go_gc_heap_allocs_by_size_bytes_bucket{le="144.99999999999997"} 475633
go_gc_heap_allocs_by_size_bytes_bucket{le="320.99999999999994"} 507361
go_gc_heap_allocs_by_size_bytes_bucket{le="704.9999999999999"} 516511
go_gc_heap_allocs_by_size_bytes_bucket{le="1536.9999999999998"} 521176
go_gc_heap_allocs_by_size_bytes_bucket{le="3200.9999999999995"} 522802
go_gc_heap_allocs_by_size_bytes_bucket{le="6528.999999999999"} 524529
go_gc_heap_allocs_by_size_bytes_bucket{le="13568.999999999998"} 525164
go_gc_heap_allocs_by_size_bytes_bucket{le="27264.999999999996"} 525269
go_gc_heap_allocs_by_size_bytes_bucket{le="+Inf"} 525421
go_gc_heap_allocs_by_size_bytes_sum 7.2408264e+07
go_gc_heap_allocs_by_size_bytes_count 525421
go_gc_heap_allocs_bytes_total 7.2408264e+07
go_gc_heap_allocs_objects_total 525421
go_gc_heap_frees_by_size_bytes_bucket{le="8.999999999999998"} 11081
go_gc_heap_frees_by_size_bytes_bucket{le="24.999999999999996"} 168291
go_gc_heap_frees_by_size_bytes_bucket{le="64.99999999999999"} 271749
go_gc_heap_frees_by_size_bytes_bucket{le="144.99999999999997"} 352424
go_gc_heap_frees_by_size_bytes_bucket{le="320.99999999999994"} 378481
go_gc_heap_frees_by_size_bytes_bucket{le="704.9999999999999"} 385700
go_gc_heap_frees_by_size_bytes_bucket{le="1536.9999999999998"} 389443
go_gc_heap_frees_by_size_bytes_bucket{le="3200.9999999999995"} 390591
go_gc_heap_frees_by_size_bytes_bucket{le="6528.999999999999"} 392069
go_gc_heap_frees_by_size_bytes_bucket{le="13568.999999999998"} 392565
go_gc_heap_frees_by_size_bytes_bucket{le="27264.999999999996"} 392636
go_gc_heap_frees_by_size_bytes_bucket{le="+Inf"} 392747
go_gc_heap_frees_by_size_bytes_sum 5.3304296e+07
go_gc_heap_frees_by_size_bytes_count 392747
go_gc_heap_frees_bytes_total 5.3304296e+07
go_gc_heap_frees_objects_total 392747
go_gc_heap_goal_bytes 3.6016864e+07
go_gc_heap_objects_objects 132674
go_gc_heap_tiny_allocs_objects_total 36033
go_gc_limiter_last_enabled_gc_cycle 0
go_gc_pauses_seconds_bucket{le="9.999999999999999e-10"} 0
go_gc_pauses_seconds_bucket{le="9.999999999999999e-09"} 0
go_gc_pauses_seconds_bucket{le="9.999999999999998e-08"} 0
go_gc_pauses_seconds_bucket{le="1.0239999999999999e-06"} 0
go_gc_pauses_seconds_bucket{le="1.0239999999999999e-05"} 1
go_gc_pauses_seconds_bucket{le="0.00010239999999999998"} 15
go_gc_pauses_seconds_bucket{le="0.0010485759999999998"} 20
go_gc_pauses_seconds_bucket{le="0.010485759999999998"} 20
go_gc_pauses_seconds_bucket{le="0.10485759999999998"} 20
go_gc_pauses_seconds_bucket{le="+Inf"} 20
go_gc_pauses_seconds_sum 0.000656384
go_gc_pauses_seconds_count 20
go_gc_stack_starting_size_bytes 4096
go_goroutines 102
go_info{version="go1.19.5"} 1
go_memory_classes_heap_free_bytes 8.839168e+06
go_memory_classes_heap_objects_bytes 1.9103968e+07
go_memory_classes_heap_released_bytes 3.530752e+06
go_memory_classes_heap_stacks_bytes 2.4576e+06
go_memory_classes_heap_unused_bytes 8.011552e+06
go_memory_classes_metadata_mcache_free_bytes 3600
go_memory_classes_metadata_mcache_inuse_bytes 12000
go_memory_classes_metadata_mspan_free_bytes 77472
go_memory_classes_metadata_mspan_inuse_bytes 426960
go_memory_classes_metadata_other_bytes 6.201928e+06
go_memory_classes_os_stacks_bytes 0
go_memory_classes_other_bytes 1.931459e+06
go_memory_classes_profiling_buckets_bytes 1.489565e+06
go_memory_classes_total_bytes 5.2086024e+07
go_memstats_alloc_bytes 1.9103968e+07
go_memstats_alloc_bytes_total 7.2408264e+07
go_memstats_buck_hash_sys_bytes 1.489565e+06
go_memstats_frees_total 428780
go_memstats_gc_sys_bytes 6.201928e+06
go_memstats_heap_alloc_bytes 1.9103968e+07
go_memstats_heap_idle_bytes 1.236992e+07
go_memstats_heap_inuse_bytes 2.711552e+07
go_memstats_heap_objects 132674
go_memstats_heap_released_bytes 3.530752e+06
go_memstats_heap_sys_bytes 3.948544e+07
go_memstats_last_gc_time_seconds 1.6992580814748092e+09
go_memstats_lookups_total 0
go_memstats_mallocs_total 561454
go_memstats_mcache_inuse_bytes 12000
go_memstats_mcache_sys_bytes 15600
go_memstats_mspan_inuse_bytes 426960
go_memstats_mspan_sys_bytes 504432
go_memstats_next_gc_bytes 3.6016864e+07
go_memstats_other_sys_bytes 1.931459e+06
go_memstats_stack_inuse_bytes 2.4576e+06
go_memstats_stack_sys_bytes 2.4576e+06
go_memstats_sys_bytes 5.2086024e+07
go_sched_gomaxprocs_threads 10
go_sched_goroutines_goroutines 102
go_sched_latencies_seconds_bucket{le="9.999999999999999e-10"} 4886
go_sched_latencies_seconds_bucket{le="9.999999999999999e-09"} 4886
go_sched_latencies_seconds_bucket{le="9.999999999999998e-08"} 5883
go_sched_latencies_seconds_bucket{le="1.0239999999999999e-06"} 6669
go_sched_latencies_seconds_bucket{le="1.0239999999999999e-05"} 7191
go_sched_latencies_seconds_bucket{le="0.00010239999999999998"} 7531
go_sched_latencies_seconds_bucket{le="0.0010485759999999998"} 7567
go_sched_latencies_seconds_bucket{le="0.010485759999999998"} 7569
go_sched_latencies_seconds_bucket{le="0.10485759999999998"} 7569
go_sched_latencies_seconds_bucket{le="+Inf"} 7569
go_sched_latencies_seconds_sum 0.00988825
go_sched_latencies_seconds_count 7569
go_threads 16
"#,
            )
            .into_bytes(),
        )));

        let _ = parser.text_to_metric_families();
        parser.pretty_metrics();
        debug!(
            "reading bytes: {}, lines: {}",
            parser.reading_bytes, parser.line_count
        );
    }

    #[test]
    fn test_basic_parse() {
        //setup_logger();
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
api_latency_seconds{method="GET",api="/v1/pull",status="url-error",quantile="0.5"} 0.000952746
api_latency_seconds{method="GET",api="/v1/pull",status="url-error",quantile="0.9"} 0.00546789
api_latency_seconds{method="GET",api="/v1/pull",status="url-error",quantile="0.99"} 0.009414857
api_latency_seconds_sum{method="GET",api="/v1/pull",status="url-error"} 0.1711532899999999
api_latency_seconds_count{method="GET",api="/v1/pull",status="url-error"} 104
api_latency_seconds{api="/v2/usage_trace",status="url-error",quantile="0.5"} 0.000855108
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
        parser.pretty_metrics();

        debug!(
            "reading bytes: {}, lines: {}",
            parser.reading_bytes, parser.line_count
        );

        debug!(
            "mf-by-name element: {:?}",
            parser
                .mf_by_name
                .get("http2_request_duration_seconds_sum")
                .unwrap()
                .borrow()
                .get_metric()
        );
    }
}
