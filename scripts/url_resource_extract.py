# pip install mysql-connector https://dev.mysql.com/doc/connector-python/en/
import mysql.connector
import RAKE  # pip install python-rake https://github.com/fabianvf/python-rake
Rake = RAKE.Rake(RAKE.SmartStopList())
from fastavro import writer, reader, parse_schema

cnx = mysql.connector.connect(user='govdroid', password='s',
                              host='govdroid',
                              database='govdroid', charset='utf8mb4', collation='utf8mb4_unicode_ci')

schema = {
    'doc': 'A crawled URL with content',
    'name': 'url_resource',
    'type': 'record',
    'fields': [
        {'name': 'url', 'type': 'string'},
        {'name': 'size_bytes', 'type': 'int'},
        {'name': 'load_time', 'type': 'float'},
        {'name': 'title', 'type': 'string'},
        {'name': 'text_content', 'type': 'string'},
        {'name': 'headings_text', 'type': 'string'},
        {'name': 'word_count', 'type': 'int'},
        {'name': 'links', 'type': {"type": 'array', "items": "string"}},
        {"name": "keywords", "type": {"type": "map", "values": "float"}}
    ],
}
parsed_schema = parse_schema(schema)

records = []



cur = cnx.cursor(dictionary=True)
cur_url = cnx.cursor(buffered=True)

cur_url.execute("select url_resource_keywords.url from url_resource_keywords where keyword = '%s'"%"retirement")
i =0
for (url,) in cur_url:
    i+=1
    print url, i, cur_url.rowcount
    record = {'keywords': {}, 'links': []}
    cur.execute("select url_resources.url as url, size_bytes, load_time, title, text_content,headings_text, word_count from url_resources inner join url_resource_text on url_resources.url=url_resource_text.url where url_resources.url = '%s'" % url)
    data = cur.fetchall()
    if len(data) > 0:
        record.update(data[0])
        for (keyword, score) in Rake.run(data[0]['text_content'], minCharacters=3, maxWords=5, minFrequency=3):
            if score > 0:
                record['keywords'][keyword] = score
        cur.execute("select url_to from url_resource_links where url_from = '%s' and url_to like 'http%%' and link_type='link'" % url)
        for link in cur:
            record['links'].append(link['url_to'])
        records.append(record)
cur.close()
cur_url.close()
cnx.close()

with open('url_resource.avro', 'wb') as out:
    writer(out, parsed_schema, records)
