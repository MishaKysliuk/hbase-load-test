package com.mapr.hbasetest.connection;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HbaseTestConnection {

  private Configuration hbaseConfig;

  public HbaseTestConnection() {
    initialize();
  }

  private void initialize() {
    Configuration conf = new Configuration();
    conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
    String hbaseSiteLocation = "/opt/mapr/hbase/hbase-1.1.8/conf/hbase-site.xml";
    conf.addResource(new Path(hbaseSiteLocation));

    hbaseConfig = HBaseConfiguration.create();
    hbaseConfig = conf;
  }

  public Configuration getHbaseConfig() {
    return hbaseConfig;
  }

  public boolean isHbaseAvailable() {
    try {
      HBaseAdmin.checkHBaseAvailable(hbaseConfig);
    } catch (ServiceException | IOException e) {
      return false;
    }
    return true;
  }
}
