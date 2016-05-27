package com.cberez.scala.hbase

import java.util
import collection.JavaConversions._

import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by cesar on 27/05/2016.
  */
object HBaseWriter {

  def createTableOrOverwrite(admin: Admin, table: HTableDescriptor): Unit = {
    if (admin.tableExists(table.getTableName)) {
      admin.disableTable(table.getTableName)
      admin.deleteTable(table.getTableName)
    }
    admin.createTable(table)
  }

  /**
    * Retrieve a row from HBase
    * @param table target table
    * @param rk row key to retrieve
    * @return the retrieved row
    */
  def getRow(table: Table, rk: String): Result = {
    val get: Get = new Get(Bytes.toBytes(rk))
    table.get(get)
  }

  /**
    * Retrieve multiple rows from HBase
    * @param table target table
    * @param rks list of row keys to retrieve
    * @return array of retrieved rows
    */
  def getRows(table: Table, rks: Array[String]): Array[Result] = {
    val gets: Seq[Get] = rks.map(key => new Get(Bytes.toBytes(key)))
    table.get(gets)
  }

  /**
    * Pretty print a row retrieved from HBase
    * @param row the row to print
    */
  def printRow(row: Result) = {
    val content: util.NavigableMap[Array[Byte], util.NavigableMap[Array[Byte], Array[Byte]]] = row.getNoVersionMap
    for (entry <- content.entrySet) {
      for (sub_entry <- entry.getValue.entrySet) {
        println(Bytes.toString(row.getRow) + "\t" + Bytes.toString(entry.getKey) + ":" + Bytes.toString(sub_entry.getKey) + " => " + Bytes.toString(sub_entry.getValue))
      }
    }
  }

  /**
    * Pretty print multiple rows retrieved from HBase
    * @param rows the rows to print
    */
  def printRows(rows: Array[Result]) = {
    rows.foreach(printRow(_))
  }

  /**
    * Insert a line to HBase
    * @param table target table
    * @param rk line's row key
    * @param cf column family's name
    * @param q column family's qualifier
    * @param value value to insert
    */
  def insertLine(table: Table, rk: String, cf: String, q: String, value: String): Unit = {
    val put = new Put(Bytes.toBytes(rk))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(q), Bytes.toBytes(value))
    table.put(put)
  }

  /**
    * Insert multiple lines into a row
    * @param table target table
    * @param rk lines row key
    * @param cf column family's name
    * @param content map of column family's qualifiers and corresponding values
    */
  def insertLines(table: Table, rk: String, cf: String, content: Map[String, String]): Unit = {
    val put = new Put(Bytes.toBytes(rk))
    content.foreach(x => put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(x._1), Bytes.toBytes(x._2)))
    table.put(put)
  }
}
