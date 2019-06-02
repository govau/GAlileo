use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::io::Read;
use warcraider::*;

use avro_rs::{to_value, types::Record, Schema, Writer};
use failure::Error;
use git_version::git_version;
use libflate::gzip::Decoder;
use log::{debug, error, info, warn};
use rayon::prelude::*;
use regex::*;
use rust_warc::WarcReader;
use soup::*;
use stackdriver_logger;
use subprocess::{Exec, Redirection};

struct WarcResult {
    url: String,
    size: i32,
    bytes: Vec<u8>,
}
fn main() -> Result<(), Error> {
    stackdriver_logger::init_with_cargo!();
    info!(
        "warcraider version {} working dir {}",
        git_version!(),
        env::current_dir()?.display()
    );
    let mut warc_number: usize;
    match env::var("WARC_NUMBER") {
        Ok(val) => warc_number = val.parse::<usize>().unwrap(),
        Err(_e) => warc_number = 1,
    }
    match env::var("OFFSET") {
        Ok(val) => warc_number += val.parse::<usize>().unwrap(),
        Err(_e) => warc_number += 0,
    }
    let offset: usize;
    match env::var("REPLICAS") {
        Ok(val) => offset = val.parse::<usize>().unwrap(),
        Err(_e) => offset = 1,
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
        // let mut first_half = true;
        let avro_filename =
            String::from("") + "dta-report02-" + warc_number.to_string().as_str() + ".avro";
        if warc_number == 59 {
            warn!("404 not found");
            warc_number += offset;
        } else if check_present_avro(&avro_filename) {
            warn!("{} already in google storage bucket", &avro_filename);
            warc_number += offset;
        } else {
            let file = io::BufWriter::new(fs::File::create(&avro_filename).unwrap());
            let mut writer = Writer::new(&schema, file);
            let warc_filename =
                String::from("") + "dta-report02-" + warc_number.to_string().as_str() + ".warc";
            download_warc(&warc_filename, warc_number);
            let f = fs::File::open(&warc_filename).expect("Unable to open file");
            let br = io::BufReader::new(f);

            let mut warc = WarcReader::new(br);
            let mut i = 0;
            loop {
                let items: Vec<WarcResult> = [
                    match warc.next() {
                        None => rust_warc::WarcRecord {
                version: String::from("0"),
                header: HashMap::<rust_warc::CaseString, String>::new(),
                /// Record content block
                content: Vec::<u8>::new(),
            },
                        Some(w) => {match w {
                        Ok(w) => w,
                        Err(_e) => rust_warc::WarcRecord {
                version: String::from("0"),
                header: HashMap::<rust_warc::CaseString, String>::new(),
                /// Record content block
                content: Vec::<u8>::new(),
            }
                        }
                        }
                    },
                 match warc.next() {
                        None => rust_warc::WarcRecord {
                version: String::from("0"),
                header: HashMap::<rust_warc::CaseString, String>::new(),
                /// Record content block
                content: Vec::<u8>::new(),
            },
                        Some(w) => {match w {
                        Ok(w) => w,
                        Err(_e) => rust_warc::WarcRecord {
                version: String::from("0"),
                header: HashMap::<rust_warc::CaseString, String>::new(),
                /// Record content block
                content: Vec::<u8>::new(),
            }
                        }
                        }
                    },  match warc.next() {
                        None => rust_warc::WarcRecord {
                version: String::from("0"),
                header: HashMap::<rust_warc::CaseString, String>::new(),
                /// Record content block
                content: Vec::<u8>::new(),
            },
                        Some(w) => {match w {
                        Ok(w) => w,
                        Err(_e) => rust_warc::WarcRecord {
                version: String::from("0"),
                header: HashMap::<rust_warc::CaseString, String>::new(),
                /// Record content block
                content: Vec::<u8>::new(),
            }
                        }
                        }
                    }, match warc.next() {
                        None => rust_warc::WarcRecord {
                version: String::from("0"),
                header: HashMap::<rust_warc::CaseString, String>::new(),
                /// Record content block
                content: Vec::<u8>::new(),
            },
                        Some(w) => {match w {
                        Ok(w) => w,
                        Err(_e) => rust_warc::WarcRecord {
                version: String::from("0"),
                header: HashMap::<rust_warc::CaseString, String>::new(),
                /// Record content block
                content: Vec::<u8>::new(),
            }
                        }
                        }
                    },
                ]
                .iter()
                .filter_map(move |item| {
                    let warc_record = item;
                    if warc_record.version != "0" && warc_record.header.get(&"WARC-Type".into()) == Some(&"response".into()) {
                        let url = String::from("")
                            + warc_record
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
                        if size > 1_000_000 || warc_record.content.len() > 1_000_000 {
                            warn!("{} too big {}", i, url);
                            None
                        }  else if url == "https://www.wgea.gov.au/sites/default/files/Workplace-profile-worksheets-2017.xlsx" {
                            warn!("{} bad url {}", i, url);
                            None
                            }else {
                            Some(WarcResult {
                                url: url,
                                size: size,
                                bytes: warc_record.content[..].to_vec(),
                            })
                        }
                    } else {
                        None
                    }
                })
                .collect();
                if items.len() == 0 {
                    info!("no more warc records");

                    break;
                }
                i += items.len();
                // if first_half && i > 50000 {
                //     first_half = false;
                //     avro_filename = String::from("")
                //         + "dta-report02-"
                //         + warc_number.to_string().as_str()
                //         + "-50000.avro";
                //     info!("{} starting new avro file {}", i, avro_filename);
                //     file = io::BufWriter::new(fs::File::create(&avro_filename).unwrap());
                //     writer = Writer::new(&schema, file);
                // }
                if i % 1000 < 10 {
                    writer.flush()?;
                }
                if i > 0 {
                    let records: Vec<Record> = items
                        .par_iter()
                        .filter_map(|item| {
                            let mut record = Record::new(writer.schema()).unwrap();
                            let url = String::from("") + item.url.as_str();
                            if i % 100 < 5 {
                                info!("{} {} ({} bytes)", i, url, item.size);
                            } else {
                                debug!("{} {} ({} bytes)", i, url, item.size);
                            }

                            record.put("url", url);
                            let mut content = String::new();
                            match Decoder::new(&item.bytes[..]) {
                                Err(_e) => error!("{} {} not valid gzip", i, item.url),
                                Ok(mut decoder) => {
                                    match decoder.read_to_string(&mut content) {
                                        Err(_e) => {
                                            error!("{} {} not valid utf8 string", i, item.url)
                                        }
                                        Ok(_e) => {
                                            let parts: Vec<&str> =
                                                content.split("\n\r\n").collect();
                                            let raw_html = String::from(parts[1]);
                                            let mut headers = HashMap::<String, String>::new();
                                            for line in parts[0].split("\n") {
                                                if line == "" || line.starts_with("HTTP/") {
                                                } else if line.contains(": ") {
                                                    let parts: Vec<&str> =
                                                        line.split(": ").collect();
                                                    headers.insert(
                                                        String::from(parts[0]),
                                                        String::from(parts[1]),
                                                    );
                                                }
                                            }

                                            record.put("size_bytes", item.size);

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
                                            let text = parse_html_to_text(&soup);
                                            let text_words = String::from("") + text.as_str();
                                            match soup.tag("title").find() {
                                                Some(title) => {
                                                    record.put("title", title.text().trim())
                                                }
                                                None => record.put("title", ""),
                                            }

                                            record.put("text_content", text);
                                            record.put(
                                                "word_count",
                                                text_words.par_split_whitespace().count() as i32,
                                            );

                                            match ga_regex.captures(&raw_html) {
                                                Some(caps) => record.put(
                                                    "google_analytics",
                                                    caps.get(0).unwrap().as_str(),
                                                ),
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

                                            record.put(
                                                "resource_urls",
                                                to_value(resource_urls(&soup)).unwrap(),
                                            );

                                            record.put(
                                                "meta_tags",
                                                to_value(meta_tags(&soup)).unwrap(),
                                            );

                                            record.put("keywords", keywords(text_words));
                                            //dbg!(record);
                                            Some(record);
                                        }
                                    }
                                }
                            }
                            None
                        })
                        .collect();
                    for record in records {
                        writer.append(record)?;
                    }
                }
            }
            writer.flush()?;
            let upload = Exec::shell("gsutil")
                .arg("cp")
                .arg(&avro_filename)
                .arg(
                    String::from("gs://us-east1-dta-airflow-b3415db4-bucket/data/bqload/")
                        + &avro_filename,
                )
                .stdout(Redirection::Pipe)
                .capture()
                .unwrap()
                .stdout_str();
            info!("{:?}", upload);
        }
        warc_number += offset;
    }
    Ok(())
}
