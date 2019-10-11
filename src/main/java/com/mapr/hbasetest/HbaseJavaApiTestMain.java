package com.mapr.hbasetest;

import com.mapr.hbasetest.connection.HbaseTestConnection;
import com.mapr.hbasetest.runnable.InsertThread;
import com.mapr.hbasetest.runnable.ScanThread;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class HbaseJavaApiTestMain {

  private static final Logger LOG = LoggerFactory.getLogger(HbaseJavaApiTestMain.class);

  public static void main(String[] args) throws InterruptedException {
    LOG.
    ExecutorService executor = Executors.newFixedThreadPool(6);
    HbaseTestConnection testConnection = new HbaseTestConnection();
    Configuration conf = testConnection.getHbaseConfig();

    List<Future<Void>> futures = submitThreeReadWrites(executor, conf);

    Thread.sleep(30000);

    cancelFutures(futures);

    executor.shutdown();

    System.out.println("Successful execution");
  }


  private static List<Future<Void>> submitThreeReadWrites(ExecutorService executorService, Configuration conf) {
    List<Future<Void>> result = new ArrayList<>();
    result.add(executorService.submit(new InsertThread(conf, "emp1")));
    result.add(executorService.submit(new InsertThread(conf, "emp2")));
    result.add(executorService.submit(new InsertThread(conf, "emp3")));
    result.add(executorService.submit(new ScanThread(conf, "emp1")));
    result.add(executorService.submit(new ScanThread(conf, "emp2")));
    result.add(executorService.submit(new ScanThread(conf, "emp3")));
    return result;
  }

  private static void cancelFutures(List<Future<Void>> futures) {
    for (Future<Void> f: futures) {
      f.cancel(true);
    }
  }

}
