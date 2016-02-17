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

  private static final String ENTITYID = "entityid";
  private static final String FINENTITY = "FinEntity";
  private static final int MIN_TRANSACTIONS = 10;
 // public static final int REPORT_MOD = 1000000;
  private static final int FETCH_SIZE = 1000000;
  private static final int UPDATE_MOD = 100;
  private static final int INSERT_ROW_MOD = 100000;
  public static final int INSERT_MOD = 500000;
  public static final int OFFSET_STEP = 4000000;

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

    long count = 0;
    long t0 = System.currentTimeMillis();

    double totalUSD = 0;
    Map<Integer, UserStats> userToStats = new HashMap<>();
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

   // UserStats userStats = userToStats.get(253977);

   // logger.info("loadTransactionTable for " + 253977 + " " + userStats);
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
   * @param info
   * @return
   * @throws Exception
   * @see BitcoinIngestUncharted#doIngest(String, String, String, String, boolean, long)
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

    Set<Integer> ids = new HashSet<>();
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

  //private static final int MB = (1024 * 1024);

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
  private long insertRowsInTable(
      String tableName,
      MysqlInfo info,
      boolean useTimestamp,
      DBConnection connection,
      Connection uncharted,
      Map<Integer, UserStats> userToStats,
      double avgUSD,
      long limit,
      Set<Integer> usersInTransactions) throws Exception {
    long count = 0;
    long t0 = System.currentTimeMillis();

    logger.debug("insertRowsInTable  " + userToStats.size() + " users into " + tableName + " limit " + limit);
    logMemory();

    int step = OFFSET_STEP;
    int offset = 0;

    String insertSQL = ingestSql.createInsertSQL(tableName, ingestSql.getColumnsForInsert());

    PreparedStatement statement = connection.getConnection().prepareStatement(insertSQL);
    Set<Integer> knownUsers = userToStats.keySet();
    logger.info("insertRowsInTable known users " + knownUsers.size());
    InsertStats insertStats = new InsertStats();

    long then2 = System.currentTimeMillis();

    while (count < limit) {
      String sql = getTransationSQL(info, step, offset);
      offset += step;
      PreparedStatement rstatement = uncharted.prepareStatement(sql);
      rstatement.setFetchSize(FETCH_SIZE);

      logMemory();

      long then = System.currentTimeMillis();
      ResultSet resultSet = rstatement.executeQuery();

      long now = System.currentTimeMillis();

      logger.info("insertRowsInTable took " + (now - then) / 1000 + " sec to do " + sql);

      logMemory();


      boolean didAny = false;
      while (resultSet.next()) {
        count++;
        didAny =true;
        insertTransaction(useTimestamp, userToStats, avgUSD, usersInTransactions, count, t0, resultSet, knownUsers, insertStats, statement);
      }
      logger.info("insertRowsInTable took " + ((System.currentTimeMillis() - then2) / 1000) + " seconds to insert " + count + " transactions.");
      logMemory();

      logger.info("insertRowsInTable skipped " + insertStats.getCountSelf() + " self transactions out of " + count);
      logger.info("insertRowsInTable skipped " + insertStats.getSkipped() + " missing users out of " + count + " and found " + knownUsers.size() + " known users");

      resultSet.close();
      rstatement.close();

      if (!didAny) {
        logger.info("\tcomplete!");
        break;
      }
    }

    statement.close();
    return count;
  }

  private String getTransationSQL(MysqlInfo info, long limit, int offset) {
    Map<String, String> slotToCol = info.getSlotToCol();
    return "select " +
        slotToCol.get(MysqlInfo.TRANSACTION_ID) +
        ", " +
        slotToCol.get(MysqlInfo.SENDER_ID) +
        ", " +
        slotToCol.get(MysqlInfo.RECEIVER_ID) +
        ", " +
        slotToCol.get(MysqlInfo.TX_TIME) +
        ", " +
        slotToCol.get(MysqlInfo.BTC) +
        ", " +
        slotToCol.get("USD") +
        " from " + info.getTable() +
        " limit " + limit +
        " offset " + offset;
  }

  private void insertTransaction(boolean useTimestamp, Map<Integer, UserStats> userToStats, double avgUSD,
                                 Set<Integer> usersInTransactions, long count, long t0,
                                 ResultSet resultSet, Set<Integer> knownUsers, InsertStats insertStats,
                                 PreparedStatement statement) throws SQLException {
    int col = 1;
    int transid  = resultSet.getInt(col++);
    int sourceid = resultSet.getInt(col++);
    int targetID = resultSet.getInt(col++);

    if (sourceid == targetID) {
      insertStats.incrSelf();
    } else if (!knownUsers.contains(sourceid) || !knownUsers.contains(targetID)) {
      insertStats.incrSkipped();
//        if (sourceid == 253977 || targetID == 253977) {
//          logger.warn("1 ---> insertRowsInTable skipped " + sourceid + " " + targetID + " " + knownUsers.contains(sourceid) + " " +knownUsers.contains(targetID));
//        }
    } else {
//        if (sourceid == 253977 || targetID == 253977) {
//          logger.warn("2 ---> insertRowsInTable inserted " + sourceid + " " + targetID + " " + knownUsers.contains(sourceid) + " " +knownUsers.contains(targetID));
//        }

      usersInTransactions.add(sourceid);
      usersInTransactions.add(targetID);

      Timestamp x = resultSet.getTimestamp(col++);
      double btc = resultSet.getDouble(col++);
      double usd = resultSet.getDouble(col++);

      double[] additionalFeatures = addAvgDollarFeatures(userToStats, avgUSD, /*count,*/ sourceid, targetID, usd);
      try {
        boolean didUpdate = insertRow(useTimestamp, t0, count, statement, additionalFeatures, transid, sourceid, targetID, x, btc, usd);

        if (!didUpdate) {
          statement.executeUpdate();
        }

      } catch (SQLException e) {
        logger.error("insertRowsInTable got error " + e + " on  " + count);
      }
      if (count % INSERT_MOD == 0) {
        long diff = System.currentTimeMillis() - t0;
        if (diff > 1000) diff /= 1000;
        logger.debug("insertRowsInTable count = " + count + "; " + count/diff+ " insert/sec");
        BitcoinFeaturesBase.logMemory();
      }
    }
  }

  private static class InsertStats {

    private int countSelf = 0;
    private int skipped = 0;

    public void incrSelf() {countSelf++;}
    public void incrSkipped() {skipped++;}

    public int getCountSelf() {
      return countSelf;
    }

    public int getSkipped() {
      return skipped;
    }
  }

  private boolean insertRow(boolean useTimestamp,
                         long t0, long count,
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

    boolean didUpdate = false;
    if (count % UPDATE_MOD == 0) {
      statement.executeUpdate();
      didUpdate = true;
    }

    if (count % INSERT_ROW_MOD == 0) {
      long diff = System.currentTimeMillis() - t0;
      if (diff > 1000) diff /= 1000;
      logger.debug("insertRow feats count = " + count + ";\t" + count/diff + " insert/sec");
      logMemory();
    }
    return didUpdate;
  }
}