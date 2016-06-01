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

package mitll.xdata.dataset.bitcoin.features;

import mitll.xdata.db.DBConnection;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by go22670 on 8/7/15.
 */
public class FeaturesSql {
  private static final Logger logger = Logger.getLogger(FeaturesSql.class);
  static final String USERS = "USERS";

  /**
   * @see BitcoinFeaturesBase#writeFeaturesToDatabase(DBConnection, Map, Map, double[][])
   * @param connection
   * @throws SQLException
   */
  void createUsersTable(Connection connection) throws SQLException {
 //   logger.info("FeaturesSql create users table");
  /*
   * Make USERS table
   */
    String createTable = getCreateSQL();
    String sqlMakeTable = "drop table " + USERS + " if exists;" + createTable;
    PreparedStatement statement = connection.prepareStatement(sqlMakeTable);
    statement.executeUpdate();
    statement.close();
  }

  private String getCreateSQL() {
    return " create table IF NOT EXISTS " +
        USERS +
        " (USER integer(10)," +
        " CREDIT_MEAN double," +
        " CREDIT_STD double," +
        " CREDIT_INTERARR_MEAN double," +
        " CREDIT_INTERARR_STD double," +
        " DEBIT_MEAN double," +
        " DEBIT_STD double," +
        " DEBIT_INTERARR_MEAN double," +
        " DEBIT_INTERARR_STD double," +
        " PERP_IN double," +
        " PERP_OUT double," +
        " TYPE int not null default(1)" +
        ")";
  }

  public void createUsersTableNoDrop(Connection connection) throws SQLException {
  //  logger.info("create users table");

  /*
   * Make USERS table
   */
    String createTable = getCreateSQL();
    PreparedStatement statement = connection.prepareStatement(createTable);
    statement.executeUpdate();
    statement.close();
  }
}
