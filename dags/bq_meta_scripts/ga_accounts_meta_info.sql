/*
  SQL Script to read metadata of Google Analytics datasets
*/

-- Not working dataset name as variable in FROM clause    
 BEGIN
 
    declare dname string;
    
    set dname = (select schema_name from
        INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE option_name = "airforce.gov.au");
    
    set dname = concat(dname,"`",".ga_sessions_*","`");
    
    select distinct geoNetwork.city
    from dname;
 
 END;


SELECT 
*
FROM `dta-ga-bigquery.dta_ga_metadata.dta_bq_datasets_desc` 
where 
   regexp_contains(schema_description, "job") = true
;


SELECT
Array
    (
      select schema_name 
      from
        INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE 
        regexp_contains(option_value, "job") = true
  ) as emp_datasets
;


-- Home Affairs
    select  schema_name, 
            regexp_replace(option_value,'\"', '') as option_name 
      from
        INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE 
        regexp_contains(option_value, "home") = true;
        

-- ATO
select  schema_name, 
            regexp_replace(option_value,'\"', '') as option_name 
      from
        INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE 
        regexp_contains(option_value, "ato|tax") = true;
/*
139405429
community.ato.gov.au

121638199
onlineservices.ato.gov.au

114274207
ato.gov.au

135414613
myato

94178846
superfundlookup.gov.au

www.dhs.gov.au
5289745
47586269
*/

select  schema_name, 
            regexp_replace(option_value,'\"', '') as option_name 
      from
        INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE schema_name in ('139405429','121638199','114274207','135414613','5289745','47586269')
        AND option_name='description';



-- DTA Properties
select  schema_name, 
            regexp_replace(option_value,'\"', '') as option_name 
      from
        INFORMATION_SCHEMA.SCHEMATA_OPTIONS 
--         order by schema_name
        WHERE 
        regexp_contains(option_value, "Style") = true
      order by schema_name;

  
 -- Extract GA datasets code, domain_name of agencies
  with tt as (  
       select  schema_name, 
            lower(regexp_replace(option_value,'\"', '')) as option_name 
       from
        INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE option_name='description'
       )
       select tt.schema_name,
              tt.option_name,
              coalesce(am.agency,'') as agency
       from tt
       left join dta_customers.domain_agency_map am
       on tt.option_name = am.domain_name
  order by am.agency, tt.schema_name     
       ;


--Check the latest GA properties connected to BQ - individual set
select  schema_name, 
            regexp_replace(option_value,'\"', '') as option_name 
      from
        INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE schema_name in ('')
        AND option_name='description';
        

--Check the latest GA properties connected to BQ - full set
select  schema_name, 
            regexp_replace(option_value,'\"', '') as option_name 
      from
        INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE regexp_contains(schema_name, "^[0-9]+") = true
        and regexp_contains(lower(option_value), "do not use") = false
        AND option_name='description'
        ;
