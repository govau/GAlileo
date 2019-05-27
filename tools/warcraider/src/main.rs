use std::io;
use std::fs;
use std::collections::HashMap;
use std::io::BufRead;

use rust_warc::WarcReader;
use avro_rs::{Schema, Writer, types::Record, to_value};
use failure::Error;
use rake::*;
use libflate::gzip::Decoder;
use soup::*;
use regex::*;
use rayon::prelude::*;
// run in prod:
// RUSTFLAGS="-C target-cpu=native" cargo run --release

fn main() -> Result<(), Error> {
    let ga_regex = Regex::new(r"\bUA-\d{4,10}-\d{1,4}\b|\bGTM-[A-Z0-9]{1,7}\b").unwrap();
    let whitespace_regex = Regex::new(r"\s+").unwrap();
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

    //println!("{:?}", schema);
    let x = "57";
    let file = fs::File::create(String::from("")+"dta-report01-"+x+".avro")?;
    let mut writer = Writer::new(&schema, file);
    let f = fs::File::open(String::from("")+"D:/warc/dta-report01-"+x+".warc").expect("Unable to open file");
    let br = io::BufReader::new(f);

    let warc = WarcReader::new(br);
let mut i = 0;
    for item in warc {
        let warc_record = item.unwrap(); // could be IO/malformed error

        if warc_record.header.get(&"WARC-Type".into()) == Some(&"response".into()) {
            let mut record = Record::new(writer.schema()).unwrap();
let url = warc_record
                    .header
                    .get(&"WARC-Target-URI".into())
                    .unwrap()
                    .as_str();
            i += 1;
            println!("{} {}", i, url);
            record.put(
                "url",
                url
            );

            let decoder = Decoder::new(&warc_record.content[..]).unwrap();

            let mut raw_html = String::from("");
            let mut headers = HashMap::<String, String>::new();
            let br = io::BufReader::new(decoder);
            let mut in_header = true;
            // TODO optimize this to just find position of first blank line
            for line in br.lines() {
                let l = line.unwrap();
                if in_header == false {
                    raw_html.push_str(l.as_str());
                } else {
                    if l == "" || l.starts_with("HTTP/") {
                    } else if l.contains(": ") {
                        let parts: Vec<&str> = l.split(": ").collect();
                        headers.insert(String::from(parts[0]), String::from(parts[1]));
                    } else {
                        in_header = false;
                    }
                }
            }
            record.put(
                "size_bytes",
                headers
                    .get("Content-Length")
                    .unwrap_or(&String::from(""))
                    .as_str()
                    .parse::<i32>()
                    .unwrap_or(0),
            );
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
            let text = whitespace_regex
                .replace_all(
                    soup.tag("body")
                        .find()
                        .unwrap()
                        .children()
                        .map(|tag| {
                            if tag.name().to_string() == "script"
                                || tag.name().to_string() == "noscript"
                                || tag.name().to_string() == "style"
                            {
                                String::from("")
                            } else {
                                tag.text().trim().to_string()
                            }
                        })
                        .collect::<Vec<String>>()
                        .join("")
                        .as_str(),
                    " ",
                )
                .to_string();

            let mut text_wc = String::from("");
            text_wc.push_str(text.as_str());
            let mut text_keyword = String::from("");
            text_keyword.push_str(text.as_str());
            match soup.tag("title").find() {
            Some(title) => record.put("title", title.text().trim()),
            None => record.put("title", "")
            }


            record.put("text_content", text);
            record.put("word_count", text_wc.par_split_whitespace().count() as i32);

            match ga_regex.captures(&raw_html) {
            Some(caps) =>   record.put("google_analytics", caps.get(0).unwrap().as_str()),
            None =>                 record.put("google_analytics", "")
            }

            let mut headings_text = String::new();
            for heading in vec!["h1", "h2", "h3", "h4", "h5", "h6"].iter() {
                for header in soup.tag(*heading).find_all() {
                    let head_text = header.text();
                    if head_text.len() > 0 {
                        headings_text.push('\n');
                        headings_text.push_str(&head_text);
                    }
                }
            }
            record.put("headings_text", headings_text);

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

            let resource_urls: Vec<String> = [
                soup.tag("script")
                    .find_all()
                    .filter_map(|link| link.get("src"))
                    .collect::<Vec<String>>(),
                soup.tag("link")
                    .find_all()
                    .filter_map(|link| link.get("href"))
                    .collect::<Vec<String>>(),
                soup.tag("img")
                    .find_all()
                    .filter_map(|link| link.get("src"))
                    .collect::<Vec<String>>(),
            ]
            .concat();
            record.put("resource_urls", to_value(resource_urls).unwrap());

            let mut meta_tags = HashMap::<String, String>::new();
            soup.tag("meta").find_all().for_each(|meta| {
                let attrs = meta.attrs();
                if attrs.contains_key("name") {
                    match attrs.get("content") {
                        Some(i) => {
                            meta_tags.insert(attrs.get("name").unwrap().to_string(), i.to_string())
                        }
                        None => Some(String::from("?")),
                    };
                } else if attrs.contains_key("http-equiv") {
                    //If http-equiv is set, it is a pragma directive — information normally given by the web server about how the web page is served.
                    match attrs.get("content") {
                        Some(i) => meta_tags
                            .insert(attrs.get("http-equiv").unwrap().to_string(), i.to_string()),
                        None => Some(String::from("?")),
                    };
                } else if attrs.contains_key("charset") {
                    //If charset is set, it is a charset declaration — the character encoding used by the webpage.
                    meta_tags.insert(
                        String::from("charset"),
                        attrs.get("charset").unwrap().to_string(),
                    );
                } else if attrs.contains_key("itemprop") {
                    //If itemprop is set, it is user-defined metadata — transparent for the user-agent as the semantics of the metadata is user-specific.
                    match attrs.get("content") {
                        Some(i) => meta_tags
                            .insert(attrs.get("itemprop").unwrap().to_string(), i.to_string()),
                        None => Some(String::from("?")),
                    };
                } else if attrs.contains_key("property") {
                    //facebook open graph

                    match attrs.get("content") {
                        Some(i) => meta_tags
                            .insert(attrs.get("property").unwrap().to_string(), i.to_string()),
                        None => Some(String::from("?")),
                    };
                }
            });
            record.put("meta_tags", to_value(meta_tags).unwrap());

            let sw = StopWords::from_file("SmartStoplist.txt").unwrap();
            let r = Rake::new(sw);
            let mut keywords = HashMap::<String, f32>::new();
            r.run(text_keyword.as_str()).iter().for_each(
                |&KeywordScore {
                     ref keyword,
                     ref score,
                 }| {
                    let mut k = String::from("");
                    k.push_str(keyword.as_str());
                    keywords.insert(k, *score as f32);
                },
            );
            record.put("keywords", keywords);
            //dbg!(record);
            writer.append(record)?;
            writer.flush()?;
        }
    }
    Ok(())
}