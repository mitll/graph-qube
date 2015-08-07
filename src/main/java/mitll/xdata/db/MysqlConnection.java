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
  private static final Logger logger = Logger.getLogger(MysqlConnection.class);

  private static final String USER = "root";
  private static final String PASSWORD = "";
  //private final String database;

  private Connection conn;

  public MysqlConnection() {
  }

  public MysqlConnection(String database) throws Exception {
    connect(database, USER, PASSWORD);
  }

  public MysqlConnection(String database, String user, String password) throws Exception {
    connect(database, user, password);
  }

  /**
   */
  public Connection connect(String database, String user, String password) throws Exception {
    String url = getSimpleURL(database);// + "?autoReconnect=true";

    return connectWithURL(url, user, password);
  }

  public String getSimpleURL(String database) {
    return "jdbc:mysql://localhost:3306/" + database;
  }

  public Connection connectWithURL(String url) {
    return connectWithURL(url, USER, PASSWORD);
  }

  private Connection connectWithURL(String url, String user, String password) {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      logger.error("got error trying to create mysql connection with URL '" + url + "', exception = " + e, e);
    }

    logger.debug("connecting to " + url);
    try {
      conn = DriverManager.getConnection(url, user, password);

      conn.setAutoCommit(true);

    } catch (SQLException e) {
      conn = null;
      logger.error("got error trying to create mysql connection with URL '" + url + "', exception = " + e, e);
    }
    return conn;
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
  public boolean isValid() {
    return conn != null;
  }

  @Override
  public String getType() {
    return "mysql";  //To change body of implemented methods use File | Settings | File Templates.
  }
}
