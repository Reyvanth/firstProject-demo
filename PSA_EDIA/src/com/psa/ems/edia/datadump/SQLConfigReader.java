package com.psa.ems.edia.datadump;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class SQLConfigReader {
  private static Properties sqlProp = new Properties();
  
  public SQLConfigReader(String filePath) throws IOException {
    EMSEdiaDBDump.printLog("[INFO]SQLConfigReader: SQL statements file [" + filePath + "]");
	InputStream propsFile = new FileInputStream(filePath);
    this.sqlProp.load(propsFile);
    propsFile.close();
  }
  
  public ArrayList getCustQueryList() {
    EMSEdiaDBDump.printLog("[INFO]SQLConfigReader@getCustQueryList: loading statements...");
    ArrayList custQueryList = new ArrayList();
    Set keySet =  this.sqlProp.keySet();
    Object[] keyArray = keySet.toArray();
    //sortKeys(keyArray);
    StringBuffer strBuffer = new StringBuffer();
    for (int i = 0; i < keyArray.length; i++) {
      String keyStr = keyArray[i].toString();
      String sqlStatement = removeQuotes(this.sqlProp.getProperty(keyStr));
      custQueryList.add(new CustSQLQuery(keyStr, sqlStatement));
      strBuffer.append(keyArray[i] + ", ");
    } 
    strBuffer.deleteCharAt(strBuffer.length() - 2);
    EMSEdiaDBDump.printLog("[INFO]SQLConfigReader@getCustQueryList: Segments Loaded [" + strBuffer + "]");
    return custQueryList;
  }
  
  private void sortKeys(Object[] keyArray) {
    for (int i = 1; i < keyArray.length; i++) {
      Object element = keyArray[i];
      int j = i - 1;
      int srcInt = Integer.parseInt(keyArray[j].toString());
      int elementInt = Integer.parseInt(element.toString());
      while (j >= 0 && srcInt > elementInt) {
        keyArray[j + 1] = keyArray[j];
        j--;
        if (j >= 0)
          srcInt = Integer.parseInt(keyArray[j].toString()); 
      } 
      keyArray[j + 1] = element;
    } 
  }
  
  private String removeQuotes(String tmpString) {
    if (tmpString == null)
      return null; 
    return tmpString.replaceAll("\"", "");
  }
}
