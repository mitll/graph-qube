package mitll.xdata.db;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 6/25/13
 * Time: 3:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class MysqlConnection implements DBConnection {
  public static final String USER = "root";
  public static final String PASSWORD = "";
  //private final String database;
  private static Logger logger = Logger.getLogger(MysqlConnection.class);

  private Connection conn;

  public MysqlConnection(String database) throws Exception { connect(database,USER,PASSWORD); }
  public MysqlConnection(String database,String user, String password) throws Exception { connect(database,user, password); }
  /**
   */
  private void connect(String database, String user, String password) throws Exception {


    String url = "jdbc:mysql:localhost";
    url = "jdbc:mysql://localhost:3306/" + database + "?autoReconnect=true";

    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      logger.error("got error trying to create mysql connection with URL '" + url + "', exception = " +e,e);
    }

    logger.debug("connecting to " + url);
    org.h2.Driver.load();
    try {
      conn = DriverManager.getConnection(url, user, password);

      conn.setAutoCommit(true);

    } catch (SQLException e) {
      conn = null;
      logger.error("got error trying to create mysql connection with URL '" + url + "', exception = " +e,e);
    }
  }

  @Override
  public Connection getConnection() {

    return conn;
  }

  @Override
  public void closeConnection() {
    try {
      logger.info("closing connection " + conn);
      if (conn != null) {
        conn.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean isValid() { return conn != null; }

  @Override
  public String getType() {
    return "mysql";  //To change body of implemented methods use File | Settings | File Templates.
  }
}
