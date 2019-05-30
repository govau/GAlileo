use warcraider::*;
use std::io;
use std::fs;
use std::io::Read;
use std::env;
use std::collections::HashMap;

use rust_warc::WarcReader;
use avro_rs::{Schema, Writer, types::Record, to_value};
use failure::Error;
use libflate::gzip::Decoder;
use soup::*;
use rayon::prelude::*;
use regex::*;
use subprocess::{Exec,Redirection};
use stackdriver_logger;
use log::{error, info, trace, debug, warn};
// run in prod:
// RUSTFLAGS="-C target-cpu=native" cargo run --release

fn main() -> Result<(), Error> {
stackdriver_logger::init_with_cargo!();

let mut warc_number: usize;
match env::var("WARC_NUMBER") {
    Ok(val) => warc_number= val.parse::<usize>().unwrap(),
    Err(_e) => warc_number=1,
}
match env::var("OFFSET") {
    Ok(val) => warc_number=warc_number+ val.parse::<usize>().unwrap(),
    Err(_e) => warc_number=warc_number+0,
}
let offset: usize;
match env::var("REPLICAS") {
    Ok(val) => offset= val.parse::<usize>().unwrap(),
    Err(_e) => offset=1,
}

    let ga_regex = Regex::new(r"\bUA-\d{4,10}-\d{1,4}\b|\bGTM-[A-Z0-9]{1,7}\b").unwrap();
    let raw_schema = r#"
        {                                                                                                        
    "name": "url_resource",                                                                                                                        
    "type": "record",                                                                                                                              
    "fields": [                                                                                                                                    
        {"name": "url", "type": "string"},                                                                                                         
        {"name": "size_bytes", "type": "int"},                                                                                                     
        {"name": "load_time", "type": "float"},                                                                                                    
        {"name": "title", "type": "string"},                                                                                                       
        {"name": "google_analytics", "type": "string"},                                                                                            
        {"name": "text_content", "type": "string"},                                                                                                
        {"name": "headings_text", "type": "string"},                                                                                               
        {"name": "word_count", "type": "int"},                                                                                                     
        {"name": "links", "type": {"type": "array", "items": "string"}},                                                                           
        {"name": "resource_urls", "type": {"type": "array", "items": "string"}},                                                                   
        {"name": "keywords", "type": {"type": "map", "values": "float"}},                                                                          
        {"name": "meta_tags", "type": {"type": "map", "values": "string"}},                                                                        
        {"name": "headers", "type": {"type": "map", "values": "string"}}                                                                           
    ]   
        }
    "#;

    let schema = Schema::parse_str(raw_schema)?;

while warc_number < 86 {
let avro_filename = String::from("") + "dta-report02-" + warc_number.to_string().as_str() + ".avro";
if warc_number == 59 {
warn!("404 not found");
warc_number=warc_number + offset;
} else if check_present_avro(&avro_filename) {
warn!("{} already in google storage bucket", &avro_filename);
warc_number=warc_number + offset;
} else {
let file = fs::File::create(&avro_filename).unwrap();
    let mut writer = Writer::new(&schema, file);
let warc_filename = String::from("") + "dta-report02-" + warc_number.to_string().as_str() + ".warc";
download_warc(&warc_filename, warc_number);
    let f = fs::File::open(&warc_filename)
        .expect("Unable to open file");
    let br = io::BufReader::new(f);

    let warc = WarcReader::new(br);
    let mut i = 0;
    for item in warc {
        i += 1;
        if i < 4 {
            let warc_record = item.unwrap(); // could be IO/malformed error

            if warc_record.header.get(&"WARC-Type".into()) == Some(&"response".into()) {
                let url = warc_record
                    .header
                    .get(&"WARC-Target-URI".into())
                    .unwrap()
                    .as_str();
                let size = warc_record
                    .header
                    .get(&"Uncompressed-Content-Length".into())
                    .unwrap_or(&String::from("0"))
                    .parse::<i32>()
                    .unwrap();
                info!("{} {} ({} bytes)", i, url, size);

                if size > 1000000 {
                    warn!("too big {}", url);
                } else {
                    let mut record = Record::new(writer.schema()).unwrap();
                    record.put("url", url);
		    let mut content = String::new();
                    let mut decoder = Decoder::new(&warc_record.content[..]).unwrap();
decoder.read_to_string(&mut content)?;
                    let parts: Vec<&str> = content.split("\n\r\n").collect();
                    let raw_html = String::from(parts[1]);
                    let mut headers = HashMap::<String, String>::new();
                    for line in parts[0].split("\n") {
                            if line == "" || line.starts_with("HTTP/") {
                            } else if line.contains(": ") {
                                let parts: Vec<&str> = line.split(": ").collect();
                                headers.insert(String::from(parts[0]), String::from(parts[1]));
                            } 
                    }

                    record.put("size_bytes", size);

                    record.put(
                        "load_time",
                        headers
                            .get("X-Funnelback-Total-Request-Time-MS")
                            .unwrap_or(&String::from(""))
                            .as_str()
                            .parse::<f32>()
                            .unwrap_or(0.0)
                            / 1000.0,
                    );
                    record.put("headers", to_value(headers).unwrap());
                    let soup = Soup::new(&raw_html);
                    let text = parse_html_to_text (&soup);
                    let text_words = String::from("") + text.as_str();
                    match soup.tag("title").find() {
                        Some(title) => record.put("title", title.text().trim()),
                        None => record.put("title", ""),
                    }

                    record.put("text_content", text);
                    record.put("word_count", *&text_words.par_split_whitespace().count() as i32);

                    match ga_regex.captures(&raw_html) {
                        Some(caps) => record.put("google_analytics", caps.get(0).unwrap().as_str()),
                        None => record.put("google_analytics", ""),
                    }

                    record.put("headings_text", headings_text(&soup));

                    record.put(
                        "links",
                        to_value(
                            soup.tag("a")
                                .find_all()
                                .filter_map(|link| link.get("href"))
                                .collect::<Vec<_>>(),
                        )
                        .unwrap(),
                    );

                    record.put("resource_urls", to_value(resource_urls(&soup)).unwrap());

                    record.put("meta_tags", to_value(meta_tags(&soup)).unwrap());

                    record.put("keywords", keywords(text_words));
                    //dbg!(record);
                    writer.append(record)?;
                    writer.flush()?;
                }
            }
        }
    }
let upload = Exec::cmd("gsutil").arg("cp").arg(&avro_filename).arg(String::from("gs://us-east1-dta-airflow-b3415db4-bucket/data/bqload/")+&avro_filename)
            .stdout(Redirection::Pipe)
            .capture().unwrap()
            .stdout_str();
        info!("{:?}", upload);
}
warc_number = warc_number + offset;
}
    Ok(())
}
