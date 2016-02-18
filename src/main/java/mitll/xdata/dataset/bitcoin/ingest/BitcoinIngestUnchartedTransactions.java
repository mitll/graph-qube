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
import mitll.xdata.dataset.bitcoin.features.Transaction;
import mitll.xdata.dataset.bitcoin.features.UserFeatures;
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

//  private static final int HOUR_IN_MILLIS = 60 * 60 * 1000;//
 // private static final long DAY_IN_MILLIS = 24 * HOUR_IN_MILLIS;

  private static final String ENTITYID = "entityid";
  private static final String FINENTITY = "FinEntity";
  private static final int MIN_TRANSACTIONS = 10;
  private static final int FETCH_SIZE = 1000000;
  private static final int UPDATE_MOD = 1000;
  private static final int INSERT_ROW_MOD = 1000000;
  private static final int INSERT_MOD = 500000;
  private static final int OFFSET_STEP = 5000000;
//  private static final int FETCH_SIZE1 = 100000;

  /**
   * Adds equivalent dollar value column
   *
   * @param info
   * @param dbType       h2 or mysql
   * @param tableName    table name to create
   * @param useTimestamp true if we want to store a sql timestamp for time, false if just a long for unix millis
   * @param limit        max number of transactions
   * @return
   * @throws Exception
   * @see BitcoinIngestUncharted#doIngest
   */
  protected Set<Integer> loadTransactionTable(
      MysqlInfo info,
      String dbType, String h2DatabaseName,
      String tableName,
      boolean useTimestamp,
      long limit,
      Collection<Integer> users,
      Map<Integer, UserFeatures> idToStats) throws Exception {
    DBConnection h2Connection = ingestSql.getDbConnection(dbType, h2DatabaseName);
    Connection uncharted = new MysqlConnection().connectWithURL(info.getJdbc());
    uncharted.setAutoCommit(false);

    if (h2Connection == null) {
      logger.error("can't handle dbtype " + dbType);
      return null;
    }

    logger.info("loadTransactionTable creating " + tableName + " in " + dbType + " given " + users.size() + " valid users");
    ingestSql.createTable(dbType, tableName, useTimestamp, h2Connection);
    logMemory();

    String unvaryingSQL = "select " +
        info.getSlotToCol().get(MysqlInfo.SENDER_ID) + ", " +
        info.getSlotToCol().get(MysqlInfo.RECEIVER_ID) + ", " +
        info.getSlotToCol().get(MysqlInfo.TX_TIME) + ", " +
        info.getSlotToCol().get(MysqlInfo.USD) +
        " from " + info.getTable();

    long count = 0;
    long t0 = System.currentTimeMillis();

    double totalUSD = 0;
    Map<Integer, UserStats> userToStats = new HashMap<>();

    logMemory();
    //  int skipped = 0;
    //  int inserted = 0;
    long start = System.currentTimeMillis();
    Map<String, Integer> stats = new HashMap<>();

    int step = OFFSET_STEP;
    int offset = 0;
    while (count < limit) {
      String sql = getOffsetSQL(unvaryingSQL, step, offset);
      offset += step;

      long beforeCount = count;
      totalUSD += doOneResultSet(uncharted, sql, users, idToStats, userToStats, stats);
      count = stats.get("count");

      long now2 = System.currentTimeMillis();

      logger.debug("loadTransactionTable took " + ((now2 - start) / 1000) + " seconds to " +count+ " " +
          //" Got past result set, skipped " + skipped +
          //" transactions with pruned users, inserted " + inserted + " : " +
          BitcoinFeaturesBase.getMemoryStatus());

      if (beforeCount == count) {
        logger.info("\tloadTransactionTable complete!");
        break;
      }
    }

    double avgUSD = totalUSD / (double) count;

    Set<Integer> usersInTransactions = new HashSet<>();
    count = insertRowsInTable(tableName, info, useTimestamp, h2Connection, uncharted, userToStats, avgUSD, limit,
        usersInTransactions);

    ingestSql.createIndices(tableName, h2Connection);
    logMemory();

    h2Connection.closeConnection();

    long t1 = System.currentTimeMillis();
    logger.debug("loadTransactionTable total count = " + count + " total time = " + ((t1 - t0) / 1000.0) + " s");
    logger.debug((t1 - 1.0 * t0) / count + " ms/insert");
    logger.debug((1000.0 * count / (t1 - 1.0 * t0)) + " inserts/s");

    return usersInTransactions;
  }

  private double doOneResultSet(Connection uncharted, String sql,

                                Collection<Integer> users,
                                Map<Integer, UserFeatures> idToStats,
                                Map<Integer, UserStats> userToStats,
                                Map<String, Integer> stats) throws SQLException {
    logger.debug("doOneResultSet executeQuery start --- " + sql);

    long then = System.currentTimeMillis();
    PreparedStatement statement = getPreparedStatement(uncharted, sql);
    ResultSet resultSet = statement.executeQuery();
    long now = System.currentTimeMillis();

    logger.debug("doOneResultSet executeQuery end  --- " + ((now-then)/1000) + " seconds ");

    int count = 0;
    double totalUSD = 0;
    long last = System.currentTimeMillis();

    while (resultSet.next()) {
      count++;

      totalUSD += useOneBitcoinRow(resultSet, users, idToStats, userToStats);
      if (count % 1000000 == 0) {
        long now2 = System.currentTimeMillis();
        logger.debug("loadTransactionTable transaction count = " + count + "; " +
       //     (now2 - t0) / count + " ms/read, and" +
            " took " +((now2-last)/1000) + " seconds : " +BitcoinFeaturesBase.getMemoryStatus());
        last = now2;
        //logMemory();
      }
    }

    stats.put("count", count + stats.getOrDefault("count", 0));

    resultSet.close();
    statement.close();
    return totalUSD;
  }

  private double useOneBitcoinRow(ResultSet resultSet,
                                  Collection<Integer> users,
                                  Map<Integer, UserFeatures> idToStats,
                                  Map<Integer, UserStats> userToStats) throws SQLException {
    int col = 1;
    int sourceid = resultSet.getInt(col++);
    int targetID = resultSet.getInt(col++);

    if (users.contains(sourceid) && users.contains(targetID)) {
      //long day = roundToDay(resultSet.getTimestamp(col++).getTime());
      long timestamp = resultSet.getTimestamp(col++).getTime();

      double dollar = resultSet.getDouble(col++);
      addUserStats(userToStats, sourceid, targetID, dollar);
      addTransaction(idToStats, sourceid, targetID, timestamp, dollar);

      //inserted++;
      return dollar;
    } else {
      return 0.0d;
      //skipped++;
    }
  }

  private PreparedStatement getPreparedStatement(Connection uncharted, String sql) throws SQLException {
    PreparedStatement preparedStatement = uncharted.prepareStatement(
        sql,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
    preparedStatement.setFetchSize(FETCH_SIZE);
    return preparedStatement;
  }

 // private long roundToDay(long time1) {
 //   return (time1 / DAY_IN_MILLIS) * DAY_IN_MILLIS;
 // }

  private void addTransaction(Map<Integer, UserFeatures> idToStats, int source, int target, long time, double amount) {
    UserFeatures sourceStats = idToStats.get(source);
    if (sourceStats == null) idToStats.put(source, sourceStats = new UserFeatures(source));
    UserFeatures targetStats = idToStats.get(target);
    if (targetStats == null) idToStats.put(target, targetStats = new UserFeatures(target));

    Transaction trans = new Transaction(source, target, time, amount);

    sourceStats.addDebit(trans);
    targetStats.addCredit(trans);
  }

  /**
   * Filter out accounts that have less than {@link #MIN_TRANSACTIONS} transactions.
   * NOTE : throws out "supernode" #25
   *
   * @param info
   * @return
   * @throws Exception
   * @see BitcoinIngestUncharted#doIngest
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
   * @param h2Connection
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
      DBConnection h2Connection,
      Connection uncharted,
      Map<Integer, UserStats> userToStats,
      double avgUSD,
      long limit,
      Set<Integer> usersInTransactions) throws Exception {
    long count = 0;
    long t0 = System.currentTimeMillis();

    logger.debug("insertRowsInTable  " + userToStats.size() + " known users into " + tableName + " limit " + limit +
        " : " + BitcoinFeaturesBase.getMemoryStatus());

    Set<Integer> knownUsers = userToStats.keySet();
    InsertStats insertStats = new InsertStats();

    long then2 = System.currentTimeMillis();

    Connection h2ConnectionConnection = h2Connection.getConnection();

    int step = OFFSET_STEP;
    int offset = 0;
    while (count < limit) {
      String sql = getTransationSQL(info, step, offset);
      offset += step;

      PreparedStatement rstatement = getPreparedStatement(uncharted,sql);

      long then = System.currentTimeMillis();
      logger.info("insertRowsInTable start query " + sql);

      ResultSet resultSet = rstatement.executeQuery();
      long now = System.currentTimeMillis();

      logger.info("insertRowsInTable end   query, took " + (now - then) / 1000 + " sec to do " + sql);

      logMemory();

      boolean didAny = false;

      String insertSQL = ingestSql.createInsertSQL(tableName, ingestSql.getColumnsForInsert());
      h2ConnectionConnection.setAutoCommit(false);
      PreparedStatement writeStatement = h2ConnectionConnection.prepareStatement(insertSQL);

      while (resultSet.next()) {
        count++;
        didAny = true;
        insertTransaction(useTimestamp, userToStats, avgUSD, usersInTransactions, count, t0, resultSet,
            knownUsers, insertStats, writeStatement);
      }
      logger.info("insertRowsInTable took " + ((System.currentTimeMillis() - then2) / 1000) +
          " seconds to insert " + count + " transactions : " + BitcoinFeaturesBase.getMemoryStatus());

      logger.info("insertRowsInTable skipped " + insertStats.getCountSelf() + " self transactions out of " + count);
      logger.info("insertRowsInTable skipped " + insertStats.getSkipped() + " missing users out of " + count + " and found " + knownUsers.size() + " known users");

      resultSet.close();
      rstatement.close();

      writeStatement.close();
      h2ConnectionConnection.commit();
      //h2ConnectionConnection.close();

      Runtime.getRuntime().gc();

      if (!didAny) {
        logger.info("\tcomplete!");
        break;
      }
    }

    return count;
  }

  private String getTransationSQL(MysqlInfo info, long limit, int offset) {
    Map<String, String> slotToCol = info.getSlotToCol();
    String table = info.getTable();
    String unvarying = "select " +
        slotToCol.get(MysqlInfo.TRANSACTION_ID) + ", " +
        slotToCol.get(MysqlInfo.SENDER_ID) + ", " +
        slotToCol.get(MysqlInfo.RECEIVER_ID) + ", " +
        slotToCol.get(MysqlInfo.TX_TIME) + ", " +
        slotToCol.get(MysqlInfo.BTC) + ", " +
        slotToCol.get(MysqlInfo.USD) +
        " from " + table;
    return getOffsetSQL(unvarying, limit, offset);
  }

  private String getOffsetSQL(String unvarying, long limit, int offset) {
    return unvarying +
        " limit " + limit +
        " offset " + offset;
  }

  /**
   * @param useTimestamp
   * @param userToStats
   * @param avgUSD
   * @param usersInTransactions
   * @param count
   * @param t0
   * @param resultSet
   * @param knownUsers
   * @param insertStats
   * @param statement
   * @throws SQLException
   * @see #insertRowsInTable(String, MysqlInfo, boolean, DBConnection, Connection, Map, double, long, Set)
   */
  private void insertTransaction(boolean useTimestamp,
                                 Map<Integer, UserStats> userToStats,
                                 double avgUSD,
                                 Set<Integer> usersInTransactions,
                                 long count,
                                 long t0,
                                 ResultSet resultSet,
                                 Set<Integer> knownUsers,
                                 InsertStats insertStats,
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

      double[] additionalFeatures = addAvgDollarFeatures(userToStats, avgUSD, sourceid, targetID, usd);
      try {
        boolean didUpdate = insertRow(useTimestamp, t0, count, statement, additionalFeatures, transid,
            sourceid, targetID, x, btc, usd);

        if (!didUpdate) {
          statement.executeUpdate();
        }

      } catch (SQLException e) {
        logger.error("insertRowsInTable got error " + e + " on  " + count);
      }
      if (count % INSERT_MOD == 0) {
        long diff = System.currentTimeMillis() - t0;
        if (diff > 1000) diff /= 1000;
        logger.debug("insertRowsInTable count = " + count + "; " + count / diff + " insert/sec");
        BitcoinFeaturesBase.logMemory();
      }
    }
  }

  private static class InsertStats {
    private int countSelf = 0;
    private int skipped = 0;

    public void incrSelf() {
      countSelf++;
    }

    public void incrSkipped() {
      skipped++;
    }

    public int getCountSelf() {
      return countSelf;
    }

    public int getSkipped() {
      return skipped;
    }
  }

  /**
   * @param useTimestamp
   * @param t0
   * @param count
   * @param statement
   * @param additionalFeatures
   * @param transid
   * @param sourceid
   * @param targetID
   * @param x
   * @param btc
   * @param usd
   * @return
   * @throws SQLException
   * @see #insertTransaction(boolean, Map, double, Set, long, long, ResultSet, Set, InsertStats, PreparedStatement)
   */
  private boolean insertRow(boolean useTimestamp,
                            long t0, long count,
                            PreparedStatement statement,
                            double[] additionalFeatures,
                            int transid,
                            int sourceid, int targetID,
                            Timestamp x, double btc, double usd) throws SQLException {
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
      logger.debug("insertRow feats count = " + count + ";\t" + count / diff + " insert/sec :\t" +
          BitcoinFeaturesBase.getMemoryStatus());
    }
    return didUpdate;
  }
}