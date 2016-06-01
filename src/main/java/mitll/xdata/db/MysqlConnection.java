/*
 * Copyright 2013-2016 MIT Lincoln Laboratory, Massachusetts Institute of Technology
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mitll.xdata.db;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

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
  private Connection connect(String database, String user, String password) throws Exception {
    String url = getSimpleURL(database);// + "?autoReconnect=true";
      logger.info("connect to " + database + " with " + user);
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

  public void unregister() {
    Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      try {
        DriverManager.deregisterDriver(driver);
        logger.info( "deregistering jdbc driver: "+driver);
      } catch (SQLException e) {
        logger.error("Error deregistering driver  "+ driver, e);
      }

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
