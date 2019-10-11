package com.mapr.hbasetest.runnable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.Callable;

public class ScanThread implements Callable<Void> {

  private Configuration conf;
  private String tableName;


  public ScanThread(Configuration conf, String tableName) {
    this.conf = conf;
    this.tableName = tableName;
  }


  @Override
  public Void call() throws Exception {
    try (Connection connection = ConnectionFactory.createConnection(this.conf);
        Table table = connection.getTable(TableName.valueOf(this.tableName))) {

      long counter = 0;
      try {
        while (!Thread.currentThread().isInterrupted()) {
          counter += tableRowsCountScanned(table);
        }
      } finally {
        System.out.println(Thread.currentThread().getName() + " scanned " + counter + " rows.");
      }
    }
    return null;
  }


  private long tableRowsCountScanned(Table table) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("country"));
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"));

    long counter = 0;

    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result result = scanner.next(); result != null; result=scanner.next()) {
        counter++;
      }
    }
    return counter;
  }
}
