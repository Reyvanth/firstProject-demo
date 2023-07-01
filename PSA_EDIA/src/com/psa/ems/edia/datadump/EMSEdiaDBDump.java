package com.psa.ems.edia.datadump;

import java.util.Date;

public class EMSEdiaDBDump {
  public static void printLog(String strLog) {
    System.out.println((new Date()).toLocaleString() + " " + strLog);
  }
  
  public static void main(String[] args) {
    long start = System.currentTimeMillis();
    String configFile = null;
    if (args.length <= 0) {
      configFile = "C:\\Users\\attreyv1\\Desktop\\Desktop\\edia\\AOIPS\\DBDump.config";
    } 
    else {
     // configFile = args[0];
      configFile = "C:\\Users\\attreyv1\\Desktop\\Desktop\\edia\\AOIPS\\DBDump.config";
    } 
    try {
      printLog(":EMS Edia DBDump Start Running..");
      System.out.println(configFile);
      DBDumpToFile dbDumpToFile = new DBDumpToFile(configFile);
      dbDumpToFile.dump();
    } catch (Exception e) {
      printLog("[Error]EMSEdiaDBDump@main: msg: " + e.getMessage());
      printLog("[Error]EMSEdiaDBDump@main: cause:" + e.getCause());
      e.printStackTrace(System.out);
    } 
    long time = System.currentTimeMillis() - start;
    printLog(": EMS Edia DBDump Stop Running..");
    printLog("Total time taken in sec = " + (time / 1000L));
  }
}
