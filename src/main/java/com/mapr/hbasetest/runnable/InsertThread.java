package com.mapr.hbasetest.runnable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Random;
import java.util.concurrent.Callable;

public class InsertThread implements Callable<Void> {

  private Configuration conf;
  private String tableName;


  public InsertThread(Configuration conf, String tableName) {
    this.conf = conf;
    this.tableName = tableName;
  }

  @Override
  public Void call() throws Exception {
    try (Connection connection = ConnectionFactory.createConnection(this.conf);
          Table table = connection.getTable(TableName.valueOf(this.tableName))) {

      Random random = new Random();
      int counter = 0;

      try {
        while (!Thread.currentThread().isInterrupted()) {
          Put put = createRandomPut(random);
          table.put(put);
          counter++;
        }
      } finally {
        System.out.println(Thread.currentThread().getName() + " inserted " + counter + " rows.");
      }
    }
    return null;
  }

  private Put createRandomPut(Random random) {
    Put put = new Put(Bytes.toBytes(random.nextLong()));

    String randName = "Name" + random.nextInt();
    String randCity = "City" + random.nextInt();
    String randCountry = "Country" + random.nextInt();

    put.addColumn(Bytes.toBytes("personal"),
        Bytes.toBytes("name"), Bytes.toBytes(randName));
    put.addColumn(Bytes.toBytes("info"),
        Bytes.toBytes("country"), Bytes.toBytes(randCountry));
    put.addColumn(Bytes.toBytes("info"),
        Bytes.toBytes("city"), Bytes.toBytes(randCity));

    return put;
  }
}
