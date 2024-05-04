use std::collections::HashMap;
use std::error::Error;

mod text_parse;

use text_parse::TextParser;

fn main() -> Result<(), Box<dyn Error>> {
    let r = std::fs::File::open("example.txt").expect("Fail to open file");
    let _parser = TextParser::new(r);

    let metric_text = r#"
     # HELP http_request_total The total number of HTTP requests.
     # TYPE http_request_total counter
     http_request_total{path="/api/v1",method="POST"} 1027
     http_request_total{path="/api/v1",method="GET"} 4711
    "#;

    // parse
    for line in metric_text.lines() {
        match parse_metric_line(line) {
            Ok((name, value, labels)) => {
                println!("Name: {}, value: {}, Labels: {:?}", name, value, labels);
            }
            Err(e) => eprintln!("Error parsing laine: {}", e),
        }
    }

    Ok(())
}

fn parse_metric_line(line: &str) -> Result<(String, f64, HashMap<String, String>), Box<dyn Error>> {
    if !line.starts_with("#") && !line.is_empty() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            let parts_except_value: Vec<&str> = parts[0].splitn(2, '{').collect();
            let value = parts[1].parse()?;

            // example: name{labels} value
            let name = parts_except_value[0].to_string();
            let labels_str = parts_except_value[1].splitn(2, '}').collect::<Vec<_>>()[0];

            let mut labels = HashMap::new();

            for part in labels_str.split(',').collect::<Vec<_>>().iter() {
                let label_pair: Vec<&str> = part.splitn(2, '=').collect();
                if label_pair.len() != 2 {
                    continue;
                }

                let label_key = label_pair[0].trim_matches('"').to_string();
                let label_val = label_pair[1].trim_matches('"').to_string();
                labels.insert(label_key, label_val);
            }

            return Ok((name, value, labels));
        }
    }

    Err("invalid metric line".into())
}
