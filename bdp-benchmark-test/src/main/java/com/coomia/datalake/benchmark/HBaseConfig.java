package com.coomia.datalake.benchmark;

import java.io.Serializable;

public class HBaseConfig implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -7388868811593786392L;
  private String zkHost;
  private String zkPort;
  private String table;
  private String cf = "cf";
  private int hbasePort;

  public String getZkHost() {
    return zkHost;
  }

  public void setZkHost(String zkHost) {
    this.zkHost = zkHost;
  }

  public String getZkPort() {
    return zkPort;
  }

  public void setZkPort(String zkPort) {
    this.zkPort = zkPort;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getCf() {
    return cf;
  }

  public void setCf(String cf) {
    this.cf = cf;
  }

  public int getHbasePort() {
    return hbasePort;
  }

  public void setHbasePort(int hbasePort) {
    this.hbasePort = hbasePort;
  }


}
