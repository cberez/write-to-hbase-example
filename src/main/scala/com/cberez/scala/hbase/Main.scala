package com.cberez.scala.hbase

import HBaseWriter._

import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.security.UserGroupInformation

object Main {
  def main(args: Array[String]): Unit = {

    // Command line job options
    val job_options: Options = new Options
    job_options.addOption("h", "help", false, "Show this help")
    job_options.addOption("t", "table-name", true, "Hbase table name")
    job_options.addOption("hs", "hbase-site", true, "hbase-site.xml file path")
    job_options.addOption("cs", "core-site", true, "hadoop core-site.xml file path")
    job_options.addOption("zp", "zk-port", true, "zookeeper quorum port (default: 2181)")
    job_options.addOption("zq", "zk-quorum", true, "zookeeper quorum (ex: host1,host2,host3)")
    job_options.addOption("p", "principal", true, "kerberos principal to use")
    job_options.addOption("k", "keytab", true, "kerberos principal's keytab to use")

    val parser: CommandLineParser = new DefaultParser
    val cmd: CommandLine = parser.parse(job_options, args)

    if (cmd.hasOption("h")) {
      val f: HelpFormatter = new HelpFormatter
      f.printHelp("Usage", job_options)
      System.exit(1)
    }

    // Prepare hbase configuration
    val conf: Configuration = HBaseConfiguration.create
    conf.addResource(new Path(cmd.getOptionValue("hs")))
    conf.addResource(new Path(cmd.getOptionValue("cs")))
    //conf.set("hbase.zookeeper.quorum", cmd.getOptionValue("zq"))
    //conf.set("hbase.zookeeper.property.clientPort", cmd.getOptionValue("zp", "2181"))
    conf.set("hadoop.security.authentication", "kerberos")

    // Login to kerberos
    println("security: " + UserGroupInformation.isSecurityEnabled)
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(cmd.getOptionValue("p"), cmd.getOptionValue("k"))


    val dossiersTableName: TableName = TableName.valueOf(cmd.getOptionValue("t"))

    try {
      println("creating connection...")

      val conn: Connection = ConnectionFactory.createConnection(conf)
      val admin: Admin = conn.getAdmin

      println("connection created")

      println("Creating table...")

      val newTable: TableName = TableName.valueOf("hbase_demo")
      val tDescriptor: HTableDescriptor = new HTableDescriptor(newTable)
      tDescriptor.addFamily(new HColumnDescriptor("CF1").setCompressionType(Algorithm.SNAPPY))
      createTableOrOverwrite(admin, tDescriptor)

      println("Table created")

      val table: Table = conn.getTable(newTable)

      println("Inserting lines in created table...")

      insertLine(table, "1", "CF1", "name", "cesar")
      insertLines(table, "2", "CF1", Map("name" -> "cesar", "age" -> "25"))

      println("Rows inserted")

      println("Reading created table...")
      printRows(getRows(table, Array("1", "2")))

      conn.close
    }
  }
}
