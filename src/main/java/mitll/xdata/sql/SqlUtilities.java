package mitll.xdata.sql;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 6/27/13
 * Time: 5:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class SqlUtilities {
  private static Logger logger = Logger.getLogger(SqlUtilities.class);

  protected int getNumColumns(Connection connection, String table) throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);

    // Get result set meta data
    ResultSetMetaData rsmd = rs.getMetaData();
    int numColumns = rsmd.getColumnCount();
    rs.close();
    stmt.close();
    return numColumns;
  }

  /**
   * @see mitll.xdata.binding.Binding#populateTableToColumns
   * @param table
   * @param connection
   * @param dbType
   * @return
   * @throws SQLException
   */
  protected Collection<String> getColumns(String table, Connection connection,String dbType ) throws SQLException {
    Set<String> columns = new HashSet<String>();
    try {
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " limit 1");

      // Get result set meta data
      ResultSetMetaData rsmd = rs.getMetaData();
      for (int i = 1; i < rsmd.getColumnCount()+1; i++) {
        String columnName = rsmd.getColumnName(i);
        columns.add(!dbType.equals("h2") ? columnName.toLowerCase(): columnName);
      }

      rs.close();
      stmt.close();
    } catch (Exception e) {
      logger.error("doing getColumns: got " +e,e);
    }
    logger.info("table " +table + " has " +columns);
    return columns;
  }

  protected Map<String, String> getNameToType(ResultSet rs) throws SQLException {
    Map<String, String> nameToType = new HashMap<String, String>();
    int columnCount = rs.getMetaData().getColumnCount();

    for (int c = 1; c <= columnCount; c++) {
      String columnName = rs.getMetaData().getColumnName(c);
      String columnTypeName = rs.getMetaData().getColumnTypeName(c);
     //   logger.debug("col " + c + " type " + columnTypeName);
      nameToType.put(columnName, columnTypeName);
    }
    return nameToType;
  }
}
