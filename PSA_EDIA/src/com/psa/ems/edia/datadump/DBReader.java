package com.psa.ems.edia.datadump;

/*
 * 03-11-2020: BCT Modification Starts
 * 
 * Oracle to SQL Conversion
 * 
 */

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import oracle.jdbc.driver.OracleDriver;

public class DBReader {
  private Connection con = null;
  
  public void initDBReader(String server, String port, String dbName, String user, String password) throws SQLException, ClassNotFoundException {
   
	//String connectionURLThin = "jdbc:oracle:thin:@" + server + ":" + port + ":" + dbName;
    //DriverManager.registerDriver((Driver)new OracleDriver());
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    
	String connectionURLThin = "jdbc:sqlserver://"+server+"\\\\MSENGRD:"+port+";user="+user+";password="+password+";sslProtocol=TLSv1.2;integratedSecurity=false;";
	
	EMSEdiaDBDump.printLog("[INFO]initDBReader: connection string [" + connectionURLThin + "]");
    
	
    this.con = DriverManager.getConnection(connectionURLThin);
  }
  
  public void initDBReader(String server, String dbName, String user, String password) throws SQLException, ClassNotFoundException {
    initDBReader(server, "1533", user, password, dbName);
  }
  
  public void initDBReader(String dbName, String user, String password) throws SQLException, ClassNotFoundException {
    initDBReader("localhost", "1533", user, password, dbName);
  }
  
  public ResultSet getResultSet(String queryString) throws SQLException {
    EMSEdiaDBDump.printLog("[INFO]DBReader@getResultSet: QUERY STRING");
    EMSEdiaDBDump.printLog(queryString);
    Statement stmt = this.con.createStatement(1005, 1007);
    ResultSet rset = stmt.executeQuery(queryString);
    return rset;
  }
}


/*
 * 03-11-2020: BCT Modification ends
 * 
 * Oracle to SQL Conversion
 * 
 */
