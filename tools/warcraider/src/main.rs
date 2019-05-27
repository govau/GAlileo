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
    let file = fs::File::create("test.avro")?;
    let mut writer = Writer::new(&schema, file);

    let f = fs::File::open("D:/warc/dta-report01-57.warc").expect("Unable to open file");
    let br = io::BufReader::new(f);

    let warc = WarcReader::new(br);

    for item in warc {
        let warc_record = item.unwrap(); // could be IO/malformed error

        if warc_record.header.get(&"WARC-Type".into()) == Some(&"response".into()) {
            let mut record = Record::new(writer.schema()).unwrap();

            record.put(
                "url",
                warc_record
                    .header
                    .get(&"WARC-Target-URI".into())
                    .unwrap_or(&String::from(""))
                    .as_str(),
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
                    .as_str(),
            );
            record.put(
                "load_time",
                headers
                    .get("X-Funnelback-Total-Request-Time-MS")
                    .unwrap_or(&String::from(""))
                    .as_str(),
            );
            dbg!(headers);

            let soup = Soup::new(&raw_html);

             record.put(
                "title",soup.tag("title").find().unwrap().text().trim()
             );
                         record.put(
                "text_content",soup.text()
             );
             //https://exercism.io/tracks/rust/exercises/word-count/solutions/9c66e8c00b794856af9f3a43f6208bde
                         record.put(
                "word_count",soup.text().par_split_whitespace().count()
             );
// {"name": "google_analytics", "type": "string"},                                                                                                     

let caps = ga_regex.captures(&raw_html).unwrap();
record.put("google_analytics",caps.get(0).unwrap().as_str());
//         {"name": "headings_text", "type": "string"},                                                                                               
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
//         {"name": "links", "type": {"type": "array", "items": "string"}},                                                                           
record.put("links",to_value(soup.tag("a").find_all().map(|link|
    match link.get("href"){
        None => String::from(""),
        Some(href) => href.as_str().to_string()
    }
).collect::<Vec<_>>()).unwrap());
//         {"name": "resource_urls", "type": {"type": "array", "items": "string"}},       
let resource_urls: Vec<String>;                                                            
// resource_urls.append (&mut soup.tag("script").find_all().map(|link|link.get("src").unwrap()).collect::<Vec<_>>());
// resource_urls.append(&mut soup.tag("link").find_all().map(|link|link.get("href").unwrap()).collect::<Vec<_>>());
// resource_urls.append(&mut soup.tag("img").find_all().map(|link|link.get("src").unwrap()).collect::<Vec<_>>());
//record.put("resource_urls",to_value(resource_urls).unwrap());
//         {"name": "meta_tags", "type": {"type": "map", "values": "string"}},        

            let sw = StopWords::from_file("SmartStoplist.txt").unwrap();
            let r = Rake::new(sw);
            let keywords = HashMap::<String,f64>::new();
            r.run(soup.text().as_str()).iter().for_each(|&KeywordScore {
                     ref keyword,
                     ref score,
                 }| println!("{}: {}", keyword, score)
            );
            record.put("keywords", keywords);

            writer.append(record)?;
            writer.flush()?;
        }
    }
    Ok(())
}