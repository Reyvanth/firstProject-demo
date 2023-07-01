package com.psa.ems.edia.datadump;

public class CustSQLQuery {
  private String sqlSegment;
  
  private String sqlQuery;
  
  public CustSQLQuery() {}
  
  public CustSQLQuery(String sqlSegment, String sqlQuery) {
    this.sqlSegment = sqlSegment;
    this.sqlQuery = sqlQuery;
  }
  
  public CustSQLQuery(String sqlQuery) {
    this.sqlSegment = "";
    this.sqlQuery = sqlQuery;
  }
  
  public String getSqlQuery() {
    return this.sqlQuery;
  }
  
  public void setSqlQuery(String sqlQuery) {
    this.sqlQuery = sqlQuery;
  }
  
  public String getSqlSegment() {
    return this.sqlSegment;
  }
  
  public void setSqlSegment(String sqlSegment) {
    this.sqlSegment = sqlSegment;
  }
}
