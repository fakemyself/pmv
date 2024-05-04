use prometheus::proto::MetricFamily;
use std::collections::HashMap;
use std::io::{self, Read};

pub struct TextParser<R: Read> {
    current_byte: u8,
    current_labels: HashMap<String, String>,
    current_bucket: f64,
    current_is_summary_count: bool,
    current_is_summary_sum: bool,
    current_is_histogram_count: bool,
    current_is_histogram_sum: bool,
    line_count: i32,
    reader: R,
    error: Option<io::Error>,
    state_fn: stateFn<R>,
}

type stateFn<R> = fn(&mut TextParser<R>) -> parserState<R>;

enum parserState<R: Read> {
    Any(stateFn<R>),
    End,
}

impl<R: Read> TextParser<R> {
    pub fn new(reader: R) -> Self {
        TextParser {
            current_labels: HashMap::new(),
            current_byte: 0 as u8,
            current_bucket: 0.0,
            current_is_summary_count: false,
            current_is_summary_sum: false,
            current_is_histogram_count: false,
            current_is_histogram_sum: false,
            line_count: 0,
            reader: reader,
            error: None,
            state_fn: TextParser::start_of_line,
        }
    }

    pub fn text_to_metric_families(
        &mut self,
        r: R,
    ) -> Result<HashMap<String, MetricFamily>, io::Error> {
        self.reset(r);

        loop {
            match (self.state_fn)(self) {
                parserState::Any(next) => {
                    self.state_fn = next;
                }
                parserState::End => {
                    break;
                }
            }
        }

        Ok(HashMap::new()) // TODO: return empty
    }

    fn reset(&self, _r: R) {
        // TODO
    }

    fn start_of_line(&mut self) -> parserState<R> {
        self.line_count += 1;
        self.skip_blank_tab();

        match self.current_byte as char {
            '#' => self.start_comment(),

            '\n' => self.start_of_line(),

            _ => self.reading_metric_name(),
        }
    }

    fn start_comment(&self) -> parserState<R> {
        parserState::End // TODO
    }

    fn reading_metric_name(&self) -> parserState<R> {
        parserState::End
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
                self.error = None; // clear error
                self.current_byte = buf[0];
            }
            Err(err) => {
                self.error = Some(err);
            }
        }
    }
}

fn is_blank_or_tab(b: u8) -> bool {
    return b == (' ' as u8) || b == ('\t' as u8);
}
