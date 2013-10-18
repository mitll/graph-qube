package mitll.xdata.db;

import java.sql.Connection;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 6/25/13
 * Time: 3:54 PM
 * To change this template use File | Settings | File Templates.
 */
public interface DBConnection {
  Connection getConnection();

  void closeConnection();

  boolean isValid();

  String getType();
}
