// LOAD CSV is great for importing small- or medium-sized data (up to 10M records).
// https://neo4j.com/developer/guide-import-csv/#batch-importer

// file directly placed in import directory (import/domaintodomain.csv)

MATCH (n) DETACH DELETE n; // clear the database before importing

DROP CONSTRAINT ON (site:Site) ASSERT site.name IS UNIQUE;
CREATE CONSTRAINT ON (site:Site) ASSERT site.name IS UNIQUE;

USING PERIODIC COMMIT // batch into 1000 rows at a time
LOAD CSV WITH HEADERS FROM "file:///domaintodomain.csv" as link
MERGE(s:Site {name: link.from_hostname})
ON CREATE SET s.linked_from = true
ON MATCH SET s.linked_from = true;

USING PERIODIC COMMIT // batch into 1000 rows at a time
LOAD CSV WITH HEADERS FROM "file:///domaintodomain.csv" as link
MERGE(s:Site {name: link.to_hostname})
ON CREATE SET s.linked_to = true
ON MATCH SET s.linked_to = true;

USING PERIODIC COMMIT // batch into 1000 rows at a time
LOAD CSV WITH HEADERS FROM "file:///domaintodomain.csv" as link
MATCH (f:Site {name: link.from_hostname})
MATCH (t:Site {name: link.to_hostname})
MERGE (f)-[r:LINKS_TO{link_count: toInteger(link.link_count)}]->(t)
RETURN count(*);

// count sites that are linked from and linked to
MATCH (n) WHERE EXISTS(n.linked_from) AND EXISTS(n.linked_to) return count(n)

// show sites linked to most that links also come from
MATCH (n)<-[]-(c) with n, count(c) as rels
  WHERE EXISTS(n.linked_from) AND EXISTS(n.linked_to)
return n.name, rels
  order by rels desc

// example query of sites DTA links to more than 25 times
MATCH(f)-[l:LINKS_TO]->(t) where f.name="dta.gov.au" and toInteger(l.link_count) > 25 return f,l,t;


