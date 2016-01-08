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

package mitll.xdata.dataset.bitcoin.ingest;

import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesBase;
import mitll.xdata.dataset.bitcoin.features.MysqlInfo;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;


/**
 * Bitcoin Ingest Class: Raw Data
 * <p>
 * Ingests raw data from CSV/TSV, populates database with users/transactions
 * and performs feature extraction
 *
 * @author Charlie Dagli, dagli@ll.mit.edu
 */
public class BitcoinIngestUnchartedTransactions extends BitcoinIngestTransactions {
  private static final Logger logger = Logger.getLogger(BitcoinIngestUnchartedTransactions.class);

  public static final String ENTITYID = "entityid";
  public static final String FINENTITY = "FinEntity";
  public static final int MIN_TRANSACTIONS = 10;
  public static final int REPORT_MOD = 1000000;
  public static final int FETCH_SIZE = 1000000;

  /**
   * Adds equivalent dollar value column
   *
   * @param info
   * @param dbType       h2 or mysql
   * @param tableName    table name to create
   * @param useTimestamp true if we want to store a sql timestamp for time, false if just a long for unix millis
   * @param limit        max number of transactions
   * @throws Exception
   * @see BitcoinIngestUncharted#doIngest
   */
  protected Set<Integer> loadTransactionTable(
      MysqlInfo info,
//      String jdbcURL, String transactionsTable,
      String dbType, String h2DatabaseName,
      String tableName,
      boolean useTimestamp,
      //Map<String, String> slotToCol,
      long limit,
      Collection<Integer> users) throws Exception {
    DBConnection connection = ingestSql.getDbConnection(dbType, h2DatabaseName);
    Connection uncharted = new MysqlConnection().connectWithURL(info.getJdbc());

    if (connection == null) {
      logger.error("can't handle dbtype " + dbType);
      return null;
    }

    logger.info("loadTransactionTable creating " + tableName + " in " + dbType + " given " + users.size() + " valid users");
    ingestSql.createTable(dbType, tableName, useTimestamp, connection);
    logMemory();

    String sql = "select " +
        info.getSlotToCol().get(MysqlInfo.SENDER_ID) +
        ", " +
        info.getSlotToCol().get(MysqlInfo.RECEIVER_ID) +
        ", " +
        info.getSlotToCol().get(MysqlInfo.USD) +
        " from " + info.getTable() + " limit " + limit;

    logger.debug("loadTransactionTable exec " + sql);
    PreparedStatement statement = uncharted.prepareStatement(sql);
    statement.setFetchSize(1000000);
    logMemory();

//    logger.debug("Getting result set --- ");
    ResultSet resultSet = statement.executeQuery();
    //  logger.debug("Got     result set --- ");

    int count = 0;
    long t0 = System.currentTimeMillis();

    double totalUSD = 0;
    Map<Integer, UserStats> userToStats = new HashMap<Integer, UserStats>();
    int mod = 1000000;

    logMemory();
    //logger.debug("Going through     result set --- ");
    int skipped = 0;
    int inserted = 0;
    while (resultSet.next()) {
      count++;

      int col = 1;
      int sourceid = resultSet.getInt(col++);
      int targetID = resultSet.getInt(col++);

      if (users.contains(sourceid) && users.contains(targetID)) {
        double dollar = resultSet.getDouble(col++);
        totalUSD += addUserStats(userToStats, sourceid, targetID, dollar);
        inserted++;
      } else {
        skipped++;
      }
      if (count % mod == 0) {
        logger.debug("loadTransactionTable transaction count = " + count + "; " + (System.currentTimeMillis() - t0) / count + " ms/read");
        logMemory();
      }
    }

    UserStats userStats = userToStats.get(253977);

    logger.info ("loadTransactionTable for " + 253977+ " "+ userStats);
    logger.debug("loadTransactionTable Got past result set, skipped " + skipped + " transactions with pruned users, inserted " + inserted);

    resultSet.close();
    statement.close();
    logMemory();

    double avgUSD = totalUSD / (double) count;

    Set<Integer> usersInTransactions = new HashSet<>();
    count = insertRowsInTable(tableName, info, useTimestamp, connection, uncharted, userToStats, avgUSD, limit, usersInTransactions);

    ingestSql.createIndices(tableName, connection);
    logMemory();

    connection.closeConnection();

    long t1 = System.currentTimeMillis();
    logger.debug("loadTransactionTable total count = " + count + " total time = " + ((t1 - t0) / 1000.0) + " s");
    logger.debug((t1 - 1.0 * t0) / count + " ms/insert");
    logger.debug((1000.0 * count / (t1 - 1.0 * t0)) + " inserts/s");

    return usersInTransactions;
  }

  /**
   * Filter out accounts that have less than {@link #MIN_TRANSACTIONS} transactions.
   * NOTE : throws out "supernode" #25
   *
   * @param connection
   * @return
   * @throws Exception
   * @see #BitcoinFeaturesUncharted(DBConnection, String, MysqlInfo, boolean, int)
   */
  Collection<Integer> getUsers(MysqlInfo info) throws Exception {
    Connection uncharted = new MysqlConnection().connectWithURL(info.getJdbc());

    long then = System.currentTimeMillis();

	  /*
     * Execute updates to figure out
	   */
    String filterUsers = "select " +
        ENTITYID +
        " from " +
        FINENTITY +
        " where numtransactions > " + MIN_TRANSACTIONS;

    PreparedStatement statement = uncharted.prepareStatement(filterUsers);
    ResultSet rs = statement.executeQuery();

    Set<Integer> ids = new HashSet<Integer>();
    int c = 0;

    while (rs.next()) {
      c++;
      if (c % 1000000 == 0) logger.debug("read  " + c);
      ids.add(rs.getInt(1));
    }
    long now = System.currentTimeMillis();
    logger.debug("getUsers took " + (now - then) + " millis to read " + ids.size() +
        " users with more than " + MIN_TRANSACTIONS + " transactions from " + FINENTITY);

    rs.close();
    statement.close();
    uncharted.close();
    return ids;
  }

  private static final int MB = (1024 * 1024);

  private void logMemory() {
    BitcoinFeaturesBase.logMemory();
  }

  /**
   * Skip self transactions.
   *
   * @param tableName
   * @param info
   * @param useTimestamp
   * @param connection
   * @param uncharted
   * @param userToStats
   * @param avgUSD
   * @param limit
   * @return
   * @throws Exception
   * @see BitcoinIngestUnchartedTransactions#loadTransactionTable
   */
  private int insertRowsInTable(
      String tableName,
      MysqlInfo info,
      boolean useTimestamp,
      DBConnection connection,
      Connection uncharted,
      //     String transactionsTable,
      Map<Integer, UserStats> userToStats,
      double avgUSD,
      //                        Map<String, String> slotToCol,
      long limit,
      Set<Integer> usersInTransactions) throws Exception {
    int count;
    count = 0;
    long t0 = System.currentTimeMillis();

    List<String> cnames = ingestSql.getColumnsForInsert();
    logger.debug("insertRowsInTable  " + userToStats.size() + " users into " + tableName + " limit " + limit);
    logMemory();

    PreparedStatement rstatement =
        uncharted.prepareStatement("select " +
            info.getSlotToCol().get(MysqlInfo.TRANSACTION_ID) +
            ", " +
            info.getSlotToCol().get(MysqlInfo.SENDER_ID) +
            ", " +
            info.getSlotToCol().get(MysqlInfo.RECEIVER_ID) +
            ", " +
            info.getSlotToCol().get(MysqlInfo.TX_TIME) +
            ", " +
            info.getSlotToCol().get(MysqlInfo.BTC) +
            ", " +
            info.getSlotToCol().get("USD") +
            " from " + info.getTable() + " limit " + limit);
    rstatement.setFetchSize(FETCH_SIZE);

    logMemory();

    ResultSet resultSet = rstatement.executeQuery();
    logMemory();

    Set<Integer> knownUsers = userToStats.keySet();

    logger.info("insertRowsInTable known users " + knownUsers.size());

    int countSelf = 0;
    int skipped = 0;
    String insertSQL = ingestSql.createInsertSQL(tableName, cnames);

//    logger.info("insertRowsInTable " + insertSQL);

    PreparedStatement statement = connection.getConnection().prepareStatement(insertSQL);
    while (resultSet.next()) {
      // double[] additionalFeatures = feats.get(count);
      count++;

      int col = 1;
      int transid = resultSet.getInt(col++);
      int sourceid = resultSet.getInt(col++);
      int targetID = resultSet.getInt(col++);

      if (sourceid == targetID) {
        countSelf++;
      } else if (!knownUsers.contains(sourceid) || !knownUsers.contains(targetID)) {
        skipped++;
        if (sourceid == 253977 || targetID == 253977) {
          logger.warn("1 ---> insertRowsInTable skipped " + sourceid + " " + targetID + " " + knownUsers.contains(sourceid) + " " +knownUsers.contains(targetID));
        }
      } else {
        if (sourceid == 253977 || targetID == 253977) {
          logger.warn("2 ---> insertRowsInTable inserted " + sourceid + " " + targetID + " " + knownUsers.contains(sourceid) + " " +knownUsers.contains(targetID));
        }

        usersInTransactions.add(sourceid);
        usersInTransactions.add(targetID);

        Timestamp x = resultSet.getTimestamp(col++);
        double btc = resultSet.getDouble(col++);

        double usd = resultSet.getDouble(col++);

        double[] additionalFeatures = addAvgDollarFeatures(userToStats, avgUSD, count, sourceid, targetID, usd);
        try {
          insertRow(useTimestamp, t0, count, statement, additionalFeatures, transid, sourceid, targetID, x, btc, usd);
        } catch (SQLException e) {
          logger.error("insertRowsInTable got error " + e + " on  " + count);
        }
        if (count % REPORT_MOD == 0) {
          logger.debug("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count
              + " ms/insert");
        }
      }
    }
    logger.info("skipped " + countSelf + " self transactions out of " + count);
    logger.info("and skipped " + skipped + " missing users out of " + count);
    rstatement.close();
    statement.close();
    return count;
  }

  private void insertRow(boolean useTimestamp,
                         long t0, int count,
                         PreparedStatement statement,
                         double[] additionalFeatures,
                         int transid, int sourceid, int targetID, Timestamp x, double btc, double usd) throws SQLException {
    int i = 1;
    statement.setInt(i++, transid);
    statement.setInt(i++, sourceid);
    statement.setInt(i++, targetID);

    if (useTimestamp) {
      statement.setTimestamp(i++, x);
    } else {
      statement.setLong(i++, x.getTime());
    }

    statement.setDouble(i++, btc);

    // do dollars
    statement.setDouble(i++, usd);
    for (double feat : additionalFeatures) statement.setDouble(i++, feat);

    statement.executeUpdate();

    if (count % 1000000 == 0) {
      logger.debug("feats count = " + count + "; " + (System.currentTimeMillis() - t0) / count
          + " ms/insert");
    }
  }
}