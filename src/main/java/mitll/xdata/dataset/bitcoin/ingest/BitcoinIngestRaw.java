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

import mitll.xdata.db.DBConnection;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Bitcoin Ingest Class: Raw Data
 * <p>
 * Ingests raw data from CSV/TSV, populates database with users/transactions
 * and performs feature extraction
 *
 * @author Charlie Dagli, dagli@ll.mit.edu
 */
public class BitcoinIngestRaw extends BitcoinIngestTransactions {
  private static final Logger logger = Logger.getLogger(BitcoinIngestRaw.class);

  /**
   * Adds equivalent dollar value column
   *
   * @param tableName    table name to create
   * @param dataFilename e.g. bitcoin-20130410.tsv
   * @param dbType       h2 or mysql
   * @param useTimestamp true if we want to store a sql timestamp for time, false if just a long for unix millis
   * @throws Exception
   * @see BitcoinIngest#main
   */
  protected void loadTransactionTable(String tableName, String dataFilename, String btcToDollarFile,
                                      String dbType, String h2DatabaseName, boolean useTimestamp) throws Exception {
    DBConnection connection = ingestSql.getDbConnection(dbType, h2DatabaseName);

    if (connection == null) {
      logger.error("can't handle dbtype " + dbType);
      return;
    }

    ingestSql.createTable(dbType, tableName, useTimestamp, connection);

    RateConverter rc = new RateConverter(btcToDollarFile);

    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
    String line;
    int count = 0;
    long t0 = System.currentTimeMillis();
    int max = Integer.MAX_VALUE;
    int bad = 0;
    //  int black = 0;
    double totalUSD = 0;
    Map<Integer, UserStats> userToStats = new HashMap<Integer, UserStats>();

    // Set<Integer> userBlacklist = new HashSet<Integer>(Arrays.asList(25)); // skip supernode 25

    while ((line = br.readLine()) != null) {
      count++;
      if (count > max) break;
      String[] split = line.split("\\s+"); // 4534248 25      25      2013-01-27 22:41:38     9.91897304
      if (split.length != 6) {
        bad++;
        if (bad < 10) logger.warn("badly formed line " + line);
      }

      int sourceid = Integer.parseInt(split[1]);
      int targetID = Integer.parseInt(split[2]);

      String day = split[3];
      Timestamp x = Timestamp.valueOf(day + " " + split[4]);

      double btc = Double.parseDouble(split[5]);

      totalUSD += addUserStats(rc, userToStats, sourceid, targetID, day, x, btc);

      if (count % 1000000 == 0) {
        logger.debug("transaction count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count
            + " ms/read");
      }
    }
    if (bad > 0) logger.warn("Got " + bad + " transactions...");
    br.close();

    double avgUSD = totalUSD / (double) count;
    List<double[]> feats = addFeatures(dataFilename, userToStats, avgUSD, rc);

    br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
    count = insertRowsInTable(tableName, useTimestamp, ingestSql.getColumnsForInsert(), connection, rc, br, feats);

    br.close();

    ingestSql.createIndices(tableName, connection);

    connection.closeConnection();

    long t1 = System.currentTimeMillis();
    logger.debug("total count = " + count);
    logger.debug("total time = " + ((t1 - t0) / 1000.0) + " s");
    logger.debug((t1 - 1.0 * t0) / count + " ms/insert");
    logger.debug((1000.0 * count / (t1 - 1.0 * t0)) + " inserts/s");
  }

  private static double toDollars(RateConverter rc, String day, Timestamp x, double btc) throws Exception {
    Double rate = rc.getConversionRate(day, x.getTime());
    return btc * rate;
  }

  private int insertRowsInTable(String tableName, boolean useTimestamp, List<String> cnames,
                                DBConnection connection,
                                RateConverter rc, BufferedReader br,  List<double[]> feats) throws Exception {
    int count;
    String line;
    count = 0;
    long t0 = System.currentTimeMillis();

    PreparedStatement statement = connection.getConnection().prepareStatement(ingestSql.createInsertSQL(tableName, cnames));

    while ((line = br.readLine()) != null) {
      double[] additionalFeatures = feats.get(count);
      count++;
      String[] split = line.split("\\s+"); // 4534248 25      25      2013-01-27 22:41:38     9.91897304

      int transid = Integer.parseInt(split[0]);
      int sourceid = Integer.parseInt(split[1]);
      int targetID = Integer.parseInt(split[2]);
      String day = split[3];
      Timestamp x = Timestamp.valueOf(day + " " + split[4]);
      double btc = Double.parseDouble(split[5]);
      double usd = toDollars(rc, day, x, btc);

      try {
        insertRow(useTimestamp, t0, count, statement, additionalFeatures, transid, sourceid, targetID, x, btc, usd);
      } catch (SQLException e) {
        logger.error("got error " + e + " on  " + line);
      }
    }
    statement.close();
    return count;
  }

  private void insertRow(boolean useTimestamp, long t0, int count,
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

  private List<double[]> addFeatures(
      String dataFilename, Map<Integer, UserStats> userToStats,
      double avgUSD,
      RateConverter rc
  ) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
    String line;
    int count = 0;
    long t0 = System.currentTimeMillis();
    int max = Integer.MAX_VALUE;
    int bad = 0;
    // List<String> cnames = Arrays.asList("DEVPOP", "CREDITDEV", "DEBITDEV");
    List<double[]> feats = new ArrayList<double[]>();

    while ((line = br.readLine()) != null) {
      count++;
      if (count > max) break;
      String[] split = line.split("\\s+"); // 4534248 25      25      2013-01-27 22:41:38     9.91897304
      if (split.length != 6) {
        bad++;
        if (bad < 10) logger.warn("badly formed line " + line);
      }

      try {
        int sourceid = Integer.parseInt(split[1]);
        int targetID = Integer.parseInt(split[2]);

        String day = split[3];
        Timestamp x = Timestamp.valueOf(day + " " + split[4]);

        double btc = Double.parseDouble(split[5]);

        // do dollars
        double usd = toDollars(rc, day, x, btc);

        feats.add(addAvgDollarFeatures(userToStats, avgUSD, /*count, */sourceid, targetID, usd));

        if (count % 1000000 == 0) {
          logger.debug("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count
              + " ms/insert");
        }
      } catch (Exception e) {
        logger.error("got " + e + " on " + line, e);
      }
    }
    if (bad > 0) logger.warn("Got " + bad + " transactions...");
    br.close();
    //statement.close();
    return feats;
  }

  private double addUserStats(RateConverter rc,
                              Map<Integer, UserStats> userToStats, int sourceid, int targetID, String day, Timestamp x, double btc) throws Exception {
    // do dollars
    double usd = toDollars(rc, day, x, btc);
    return addUserStats(userToStats, sourceid, targetID, usd);
  }
}