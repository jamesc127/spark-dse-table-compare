spark-dse-table-compare
====
# Overview
The intention of this repo is to be used with a DSE Analytics cluster to compare the data in two tables that have the same schema. 
The output of the spark job will be a `tsv` (tab separated) file which will be completely empty if there are no differences, or one line for each difference between the two tables.
If any differences are found, the values from both tables will be printed into the `tsv` on the same row.
Each column of the `tsv` will be prefixed with either `t1` or `t2` corresponding to either `master_table` or `compare_table` respectively so you know which value came from where.
The number of columns in the `tsv` will be
```bash
(number of columns in t1 + number of columns in t2)
```
In an attempt to avoid some data type issues that we encountered early in development, the comparison is done by using the Spark SQL function `hash` on each row of the table. 
The tables are then compared using `EXCEPT` on just their primary key columns and the hash. 
The output of the comparison (the rows that are different between the tables) are then re-joined with their original tables and then combined into the output.

#### Sample Output

|t1_combinable_with_other_item|t2_combinable_with_other_item|t1_country_code|t2_country_code|t1_item_id|t2_item_id|
|---                          |---                          |---            |---            |---       |---       |
|                        false|                         true|             US|             US|      1139|      1139|
|                         true|                         true|             US|             MX|      1133|      1133|
|                         true|                        false|             US|             US|      1135|      1135|
|                         true|                         true|             CA|             US|      1137|      1137|
|                        false|                         true|             US|             US|      1131|      1131|

# Configuration File
- `master_table` : table name and keyspace of the primary table in the comparison, although since the tables have the same schema there is essentially no difference (one of the tables has to be first, right?).
```json
{
  "master_table"        : {
    "keyspace"          : "compare",
    "table"             : "price_tiers_by_catalog_code_country_item"
  }
}
```
- `compare_table` : table name and keyspace of the second table in the comparison.
```json
{
  "compare_table"       : {
    "keyspace"          : "compare",
    "table"             : "price_tiers_by_catalog_code_country_item_dse"
  }
}
```
- `primary_key_columns` : `JSON` array of column names in the table's `PRIMARY KEY`. All PK columns should be listed to ensure the correct rows get compared.
```json
{
  "primary_key_columns" : [
    "system_country",
    "catalog_code",
    "item_number"
  ]
}
```
- `exclude_columns` is a string list of columns you wish to exclude from the comparison. Since the tables have the same schema, the columns will be dropped from both tables. They will not be present in the output file of the job.
```json
{  
"exclude_columns"     : [
    "last_changed"
  ]
}
```
- `csv_path` is for the output path of the resulting csv file. Best stored on `dsefs` unless testing locally.
```json
{
  "csv_path"            : "file:///Users/jamescolvin/Downloads/dse-6.7.5/lib/data/spark/item_by_item_id_diff"
}
```
# Data Type Conversions
Several times in the testing process we ran into issues with Spark data types being incompatible with the output `tsv` or with some part of the comparison.
# Spark-Submit
- Follow the instructions on [spark-submit](https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/tools/dse/dseSpark-submit.html)
- Submit on the cluster spark master
- add `--driver-memory=`, `--driver-cores=`, `--executor-memory=`, or `--executor-cores=` to `spark-submit` as necessary to support the successful operation of your spark job.
- `spark-submit` should look something like the following: 
```bash
dse spark-submit --files /path/to/application.json \
--conf spark.executor.extraJavaOptions=-Dconfig.file=/path/to/application.json \
--conf spark.driver.extraJavaOptions=-Dconfig.file=/path/to/application.json \
--deploy-mode cluster \
--supervise \
--class "TableCompare" /path/to/spark-table-compare.jar
```
## Building
Any updates should be built with `sbt assembly`. Get [sbt assembly](https://github.com/sbt/sbt-assembly) here.

## TODO
- [ ] add in blurbs about column conversions

### Thanks
Many thanks to [Jim Hatcher](https://github.com/jhatcher9999) and [Alex Ott](https://github.com/alexott) for help and inspiration with this effort.