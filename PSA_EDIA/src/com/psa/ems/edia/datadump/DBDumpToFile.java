package com.psa.ems.edia.datadump;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

public class DBDumpToFile {
  private String outputDir = ".";
  
  private String backupDir = ".";
  
  private String logDir = ".";
  
  private String dbUser = "";
  
  private String dbPassword = "";
  
  private String dbServerName = "";
  
  private String dbName = "";
  
  private String sqlFilePath = "";
  
  private boolean checkSum = false;
  
  private DBReader dbReader;
  
  private ArrayList custQueryList = null;
  
  private final String DELIMITER = ";";
  
  public DBDumpToFile(String configFilePath) throws IOException {
    System.out.println(" ");
    EMSEdiaDBDump.printLog("[INFO]DBDumpToFile  : config file [" + configFilePath + "]");
    loadProperties(configFilePath);
    this.dbReader = new DBReader();
    fillCustQueryList();
  }
  
  private void loadProperties(String configFilePath) throws IOException {
    Properties tempProp = new Properties();
    InputStream propsFile = new FileInputStream(configFilePath);
    tempProp.load(propsFile);
    propsFile.close();
    tempProp.list(System.out);
    this.dbUser = removeQuotes(tempProp.getProperty("user"));
    this.dbPassword = removeQuotes(tempProp.getProperty("pass"));
    this.dbServerName = removeQuotes(tempProp.getProperty("dbserver"));
    this.dbName = removeQuotes(tempProp.getProperty("dbname"));
    this.outputDir = removeQuotes(tempProp.getProperty("output_dir"));
    this.backupDir = removeQuotes(tempProp.getProperty("backup_dir"));
    this.logDir = removeQuotes(tempProp.getProperty("log_dir"));
    this.sqlFilePath = removeQuotes(tempProp.getProperty("sql_file"));
    String tmpCheckSumStr = removeQuotes(tempProp.getProperty("checksum"));
    validateProperties();
    if (tmpCheckSumStr.toLowerCase() == "y") {
      this.checkSum = true;
    } else {
      this.checkSum = false;
    } 
    this.outputDir = appendSlashOnDirString(this.outputDir);
    this.backupDir = appendSlashOnDirString(this.backupDir);
    this.logDir = appendSlashOnDirString(this.logDir);
  }
  
  private void validateProperties() {
    if (this.outputDir == null) {
      EMSEdiaDBDump.printLog("[ERROR] cannot read property [outputDir],using default value ");
      this.outputDir = ".";
    } 
    if (this.backupDir == null) {
      EMSEdiaDBDump.printLog("[ERROR] cannot read property [backupDir],using default value ");
      this.backupDir = ".";
    } 
    if (this.logDir == null) {
      EMSEdiaDBDump.printLog("[ERROR] cannot read property [logDir],using default value ");
      this.logDir = ".";
    } 
    if (this.dbUser == null) {
      EMSEdiaDBDump.printLog("[ERROR] cannot read property [dbUser],using default value ");
      this.dbUser = "";
    } 
    if (this.dbPassword == null) {
      EMSEdiaDBDump.printLog("[ERROR] cannot read property [dbPassword],using default value ");
      this.dbPassword = "";
    } 
    if (this.dbServerName == null) {
      EMSEdiaDBDump.printLog("[ERROR] cannot read property [dbServerName],using default value ");
      this.dbServerName = "localhost";
    } 
    if (this.sqlFilePath == null) {
      EMSEdiaDBDump.printLog("[ERROR] cannot read property [sql_file],using default value ");
      this.sqlFilePath = "DBDumpSQL.config";
    } 
    if (this.dbName == null) {
      EMSEdiaDBDump.printLog("[ERROR] cannot read property [dbName],using default value ");
      this.dbName = "engrs";
    } 
  }
  
  private void fillCustQueryList() throws IOException {
    SQLConfigReader sqlConfigReader = new SQLConfigReader(this.sqlFilePath);
    this.custQueryList = sqlConfigReader.getCustQueryList();
  }
  
  public void dump() throws IOException, ClassNotFoundException {
    try {
      this.dbReader.initDBReader(this.dbServerName, "1533", this.dbName, this.dbUser, this.dbPassword);
      String fileName = genFileName();
      EMSEdiaDBDump.printLog("[INFO]DBDumpToFile@dump: Output File[" + fileName + "]");
      FileWriter fileWriter = new FileWriter(String.valueOf(this.outputDir) + fileName);
      PrintWriter outputStream = new PrintWriter(fileWriter);
      for (Iterator<CustSQLQuery> it = this.custQueryList.iterator(); it.hasNext(); ) {
        CustSQLQuery query = it.next();
        dumpRecordsToFile(query, outputStream);
      } 
      outputStream.close();
    } catch (SQLException e) {
      EMSEdiaDBDump.printLog("[Error]DBDumpToFile@dump: msg: " + e.getMessage());
      EMSEdiaDBDump.printLog("[Error]DBDumpToFile@dump: cause:" + e.getCause());
      e.printStackTrace(System.out);
    } 
  }
  
  private String genFileName() {
    Date currentDate = new Date();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMM");
    SimpleDateFormat dateFormatDD = new SimpleDateFormat("yyyyMMdd");
    
    //Previous Month Calculation
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MONTH, -1);
    Date previousMonthDate = calendar.getTime();
    String previousMonth = dateFormat.format(previousMonthDate);    
    
    String fileName = "ems_edia_aoips_" + previousMonth+"_"+dateFormatDD.format(currentDate);
    return fileName;
  }
  
  private void dumpRecordsToFile(CustSQLQuery query, PrintWriter outputStream) throws SQLException, IOException {
    ResultSet rset = this.dbReader.getResultSet(query.getSqlQuery());
   
    /*Date currentDate = new Date();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMM");
    String currentDate_f = dateFormat.format(currentDate);*/
    
    //Previous Month Calculation
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MONTH, -1);
    Date previousMonthDate = calendar.getTime();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMM");
    String previousMonth = dateFormat.format(previousMonthDate); 
    
    
    DecimalFormat decFormat = new DecimalFormat("000000");
    rset.last();
    int rowCount = rset.getRow();
    StringBuffer strBuffer = new StringBuffer();
    ResultSetMetaData rsMetaData = rset.getMetaData();
    int selectFieldsCount = rsMetaData.getColumnCount();
    rset.beforeFirst();
    ArrayList<String> strList = new ArrayList();
    while (rset.next()) {
      boolean isNullRow = false;
      if (strBuffer.length() != 0)
        strBuffer.delete(0, strBuffer.length()); 
      for (int i = 1; i <= selectFieldsCount; i++) {
        String strTmp = formatNumField(rset.getString(i));
        if (strTmp == null) {
          rowCount--;
          isNullRow = true;
          break;
        } 
        strBuffer.append(strTmp);
        strBuffer.append(";");
      } 
      if (isNullRow)
        continue; 
      strBuffer.deleteCharAt(strBuffer.length() - 1);
      strList.add(strBuffer.toString());
    } 
    /*
     * BCT: Updated For AOIPS File
     */
    strBuffer = new StringBuffer();
    strBuffer.append("0");
    strBuffer.append(";");
    strBuffer.append(query.getSqlSegment());
    strBuffer.append(";");
    strBuffer.append(previousMonth);
    strBuffer.append(";");
    strBuffer.append(decFormat.format(rowCount));
    EMSEdiaDBDump.printLog("[INFO]DBDumpToFile@dumpRecordsToFile: query num~record count - " + strBuffer.toString());
    outputStream.println(strBuffer.toString());
    for (Iterator<String> it = strList.iterator(); it.hasNext();)
      outputStream.println(it.next().toString()); 
  }
  
  private String removeQuotes(String tmpString) {
    if (tmpString == null)
      return null; 
    return tmpString.replaceAll("\"", "");
  }
  
  private String appendSlashOnDirString(String dirString) {
    if (dirString.charAt(dirString.length() - 1) != '\\' || dirString.charAt(dirString.length() - 1) != '/')
      dirString = String.valueOf(dirString) + "/"; 
    return dirString;
  }
  
  public String formatNumField(String strField) {
    String strReturn = strField;
    try {
      Double dblNum = Double.valueOf(strField);
      DecimalFormat decFormat = new DecimalFormat("#######.###############");
      strReturn = decFormat.format(dblNum);
      Integer.valueOf(strField);
    } catch (Exception e) {
      return strReturn;
    } 
    return strField;
  }
}
