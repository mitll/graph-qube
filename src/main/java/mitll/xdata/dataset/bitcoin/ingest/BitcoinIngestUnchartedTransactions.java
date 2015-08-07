// Copyright 2013-2015 MIT Lincoln Laboratory, Massachusetts Institute of Technology 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mitll.xdata.dataset.bitcoin.ingest;

import mitll.xdata.db.DBConnection;
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
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

  /**
   * Adds equivalent dollar value column
   *
   * @param bitcoinDB
   * @param dbType       h2 or mysql
   * @param tableName    table name to create
   * @param useTimestamp true if we want to store a sql timestamp for time, false if just a long for unix millis
   * @throws Exception
   * @see BitcoinIngestUncharted#main
   */
  protected void loadTransactionTable(String bitcoinDB, String transactionsTable, String dbType, String h2DatabaseName, String tableName,
                                      boolean useTimestamp) throws Exception {
    DBConnection connection = ingestSql.getDbConnection(dbType, h2DatabaseName);
    DBConnection bitcoinData = new MysqlConnection(bitcoinDB);

    if (connection == null) {
      logger.error("can't handle dbtype " + dbType);
      return;
    }

    ingestSql.createTable(dbType, tableName, useTimestamp, connection);
    logMemory();

    Connection uncharted = bitcoinData.getConnection();
  //  PreparedStatement statement = getStatement(uncharted, transactionsTable);

    int limit = 20000000;

    String sql = "select SenderId, ReceiverID, USD from " + transactionsTable + " limit " + limit;
    logger.debug("exec " + sql);
    PreparedStatement statement = uncharted.prepareStatement(sql);
    logMemory();

    logger.debug("Getting result set --- ");
    ResultSet resultSet = statement.executeQuery();
    logger.debug("Got     result set --- ");

    int count = 0;
    long t0 = System.currentTimeMillis();
 //   int max = Integer.MAX_VALUE;
//    int bad = 0;
    double totalUSD = 0;
    Map<Integer, UserStats> userToStats = new HashMap<Integer, UserStats>();
    int mod = 100000;

    logMemory();
    logger.debug("Going through     result set --- ");

    while (resultSet.next()) {
      count++;

      int col = 1;
      int sourceid = resultSet.getInt(col++);
      int targetID = resultSet.getInt(col++);

      double dollar = resultSet.getDouble(col++);

      totalUSD += addUserStats(userToStats, sourceid, targetID, dollar);

      if (count % mod == 0) {
        logger.debug("transaction count = " + count + "; " + (System.currentTimeMillis() - t0) / count + " ms/read");
        logMemory();
      }
    }
    logger.debug("Got past result set ");

    resultSet.close();
    statement.close();
    logMemory();

    double avgUSD = totalUSD / (double) count;

    count = insertRowsInTable(tableName, useTimestamp,  connection, uncharted, transactionsTable,userToStats,avgUSD,limit);

    ingestSql.createIndices(tableName, connection);
    logMemory();

    connection.closeConnection();

    long t1 = System.currentTimeMillis();
    logger.debug("total count = " + count);
    logger.debug("total time = " + ((t1 - t0) / 1000.0) + " s");
    logger.debug((t1 - 1.0 * t0) / count + " ms/insert");
    logger.debug((1000.0 * count / (t1 - 1.0 * t0)) + " inserts/s");
  }
  private static final int MB = (1024 * 1024);

  private void logMemory() {
    Runtime rt = Runtime.getRuntime();
    long free = rt.freeMemory();
    long used = rt.totalMemory() - free;
    long max = rt.maxMemory();
    logger.debug("heap info free " + free / MB + "M used " + used / MB + "M max " + max / MB + "M");
    //return free;
  }

  private PreparedStatement getStatement(Connection uncharted, String transactionsTable, int limit) throws SQLException {
    return uncharted.prepareStatement("select SenderId, ReceiverID, TxTime, BTC, USD from " + transactionsTable + " limit " + limit);
  }

  private int insertRowsInTable(String tableName, boolean useTimestamp,
                                DBConnection connection,
                                Connection uncharted,
                                String transactionsTable,
                                Map<Integer, UserStats> userToStats,
                                double avgUSD,
                                 int limit) throws Exception {
    int count;
    count = 0;
    long t0 = System.currentTimeMillis();

    List<String> cnames = ingestSql.getColumnsForTransactionsTable();
    PreparedStatement statement = connection.getConnection().prepareStatement(ingestSql.createInsertSQL(tableName, cnames));


    PreparedStatement rstatement =
        uncharted.prepareStatement("select TransactionId, SenderId, ReceiverID, TxTime, BTC, USD from " + transactionsTable+ " limit " + limit);
    logMemory();

    ResultSet resultSet = rstatement.executeQuery();
    logMemory();

    while (resultSet.next()) {
      // double[] additionalFeatures = feats.get(count);
      count++;

      int col = 1;
      int transid = resultSet.getInt(col++);
      int sourceid = resultSet.getInt(col++);
      int targetID = resultSet.getInt(col++);

      Timestamp x = resultSet.getTimestamp(col++);
      double btc = resultSet.getDouble(col++);

      double usd = resultSet.getDouble(col++);

      double[] additionalFeatures = addAvgDollarFeatures(userToStats, avgUSD, count, sourceid, targetID, usd);
      try {
        insertRow(useTimestamp, t0, count, statement, additionalFeatures, transid, sourceid, targetID, x, btc, usd);
      } catch (SQLException e) {
        logger.error("got error " + e + " on  " + count);
      }
      if (count % 1000000 == 0) {
        logger.debug("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count
            + " ms/insert");
      }
    }
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
    //logger.info(additionalFeatures);
    for (double feat : additionalFeatures) statement.setDouble(i++, feat);
    statement.executeUpdate();

    if (count % 1000000 == 0) {
      logger.debug("feats count = " + count + "; " + (System.currentTimeMillis() - t0) / count
          + " ms/insert");
    }
  }

/*  private List<double[]> addFeatures(
      Connection uncharted,
      String transactionsTable,
      Map<Integer, UserStats> userToStats,
      double avgUSD
  ) throws Exception {
    PreparedStatement statement = getStatement(uncharted, transactionsTable);
    ResultSet resultSet = statement.executeQuery();

    int count = 0;
    long t0 = System.currentTimeMillis();
    // List<String> cnames = Arrays.asList("DEVPOP", "CREDITDEV", "DEBITDEV");
    List<double[]> feats = new ArrayList<double[]>();

    while (resultSet.next()) {
      count++;

      try {
        int col = 1;
        int sourceid = resultSet.getInt(col++);
        int targetID = resultSet.getInt(col++);

        Timestamp x = resultSet.getTimestamp(col++);
        double btc = resultSet.getDouble(col++);

        double usd = resultSet.getDouble(col++);

        feats.add(addAvgDollarFeatures(userToStats, avgUSD, count, sourceid, targetID, usd));

        if (count % 1000000 == 0) {
          logger.debug("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count
              + " ms/insert");
        }
      } catch (Exception e) {
        logger.error("got " + e, e);
      }
    }

    statement.close();
    return feats;
  }*/
}