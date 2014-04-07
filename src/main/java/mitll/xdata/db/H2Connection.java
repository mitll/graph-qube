package mitll.xdata.db;

import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Makes H2 Connections.
 *
 * User: GO22670
 * Date: 12/31/12
 * Time: 5:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class H2Connection implements DBConnection {
  private static Logger logger = Logger.getLogger(H2Connection.class);

  private Connection conn;
 // private int cacheSizeKB;
  private int queryCacheSize;
  private int maxMemoryRows = 50000;

  public H2Connection( String dbName) {
    this(".", dbName, 50000, 8,50000, false);
  }
  public H2Connection( String dbName,int maxMemoryRows) {
    this(".", dbName, 50000, 8,maxMemoryRows, false);
  }
  public H2Connection( String dbName,int maxMemoryRows,boolean createDB) {
    this(".", dbName, 50000, 8,maxMemoryRows, createDB);
  }
  public H2Connection( String dbName, boolean createDB) {
    this(".", dbName, 50000, 8,50000,createDB);
  }
  public H2Connection(String configDir, String dbName) {
    this(configDir, dbName, 50000, 8,5000000,false);
  }
    /**
     * @param configDir
     * @param dbName
     */
  public H2Connection(String configDir, String dbName, int cacheSizeKB, int queryCacheSize, int maxMemoryRows,boolean createDB) {
  //  this.cacheSizeKB = cacheSizeKB;
    this.queryCacheSize = queryCacheSize;
    this.maxMemoryRows = maxMemoryRows;
    connect(configDir, dbName,createDB);
  }

  private void connect(String configDir, String database, boolean createDB) {
    String h2FilePath = configDir +File.separator+ database;
    connect(h2FilePath,createDB);
  }

  /**
   *   //jdbc:h2:file:/Users/go22670/DLITest/clean/netPron2/war/config/urdu/vlr-parle;IFEXISTS=TRUE;CACHE_SIZE=30000
   * @param h2FilePath
   */
  private void connect(String h2FilePath, boolean create) {
    String url = "jdbc:h2:file:" + h2FilePath + ";"+
        (create ? "" : "IFEXISTS=TRUE;" )+
        "LOG=0;CACHE_SIZE=65536;LOCK_MODE=0;"+
      "QUERY_CACHE_SIZE=" + queryCacheSize + ";" +
    //    "CACHE_SIZE="       + cacheSizeKB + ";" +
        "DATABASE_TO_UPPER=false"    + ";" +
      "MAX_MEMORY_ROWS="  +maxMemoryRows
      ;

    logger.debug("connecting to " + url);
    org.h2.Driver.load();
    try {
      conn = DriverManager.getConnection(url, "", "");
      conn.setAutoCommit(true);

    } catch (SQLException e) {
      conn = null;
      logger.error("got error trying to create h2 connection with URL '" + url + "', exception = " +e,e);
    }
  }

  public void contextDestroyed() {
    if (conn == null) {
      logger.info("not never successfully created h2 connection ");
    } else {
      sendShutdown();
      closeConnection();
      org.h2.Driver.unload();
    }
  }

  private void sendShutdown() {
    logger.info("send shutdown on connection " + conn);

    try {
      Statement stat = conn.createStatement();
      stat.execute("SHUTDOWN");
      stat.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

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

  public Connection getConnection() { return conn; }
  public boolean isValid() { return conn != null; }

  @Override
  public String getType() {
    return "h2";  //To change body of implemented methods use File | Settings | File Templates.
  }
}
