# Scala write to HBase example

An example project showing how to read and write from a Kerberized HBase using the
1.0 API and how to parse the results.

Based on lucasbak's [kafka-spark-streaming](https://github.com/lucasbak/kafka-spark-streaming)
use of HBase's 0.97 API with kerberos and [HBase's cookbook](http://hbase.apache.org/book.html#hbase_apis)

## What does it do ?

*Basics !*

The app creates a table called `hbase_demo` with the given kerberos principal (it needs
to have `RWC` rights on the table in hbase)

Then it inserts three lines / two rows, reads and prints them

## Run

The project needs to be compiled with [Maven](https://maven.apache.org/) :

```bash
cd write-to-hbase
mvn package
```

It has to be ran with the following options :

```bash
 -cs,--core-site <arg>    hadoop core-site.xml file path
 -hs,--hbase-site <arg>   hbase-site.xml file path
 -k,--keytab <arg>        kerberos principal's keytab to use
 -p,--principal <arg>     kerberos principal to use
```