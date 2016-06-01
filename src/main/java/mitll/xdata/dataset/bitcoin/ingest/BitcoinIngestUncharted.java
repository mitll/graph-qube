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

import mitll.xdata.ServerProperties;
import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesBase;
import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesUncharted;
import mitll.xdata.dataset.bitcoin.features.MysqlInfo;
import mitll.xdata.dataset.bitcoin.features.UserFeatures;
import mitll.xdata.db.H2Connection;
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Performs all data ingest for Bitcoin data:
 * - Ingest raw transactions CSV
 * - Extract account features
 * - Build SubGraph Search Indices and Features
 */
public class BitcoinIngestUncharted extends BitcoinIngestBase {
  private static final Logger logger = Logger.getLogger(BitcoinIngestUncharted.class);

  private static final boolean USE_TIMESTAMP = false;
  //  private static final String BITCOIN = "bitcoin";
  //private static final String USERTRANSACTIONS_2013_LARGERTHANDOLLAR = "usertransactions2013largerthandollar";
  private static final String SKIP_TRUE = "skip=true";

  /**
   * Remember to give lots of memory if running on fill bitcoin dataset -- more than 2G
   * <p>
   * arg 0 is the datafile input
   * arg 1 is the db to write to
   * arg 2 is the directory to write the feature files to
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Throwable {
    //
    // Parse Arguments...
    //
    boolean skipLoadTransactions = false;
    String dbName = null;//"bitcoin";
    String writeDir = "outUncharted";
    String propsFile = null;
//    long limit = 20000000000l;
    long limit = 20000l;
    for (String arg : args) {
      limit = getLimit(limit, arg);

      String prefix = "db=";
      if (arg.startsWith(prefix)) {
        dbName = getValue(arg, prefix);
      }

      prefix = "out=";
      if (arg.startsWith(prefix)) {
        writeDir = getValue(arg, prefix);
      }

      prefix = "skip=";
      if (arg.startsWith(prefix)) {
        skipLoadTransactions = arg.equals(SKIP_TRUE);
      }

      prefix = "props=";
      logger.info("got " + arg);

      if (arg.startsWith(prefix)) {
        propsFile = getValue(arg, prefix);
      } else if (arg.startsWith("-" + prefix)) {
        propsFile = getValue(arg, "-" + prefix);
      }
    }

    if (propsFile == null) {
      logger.error("please set a props file with props=<file>");
      return;
    }
    BitcoinFeaturesBase.logMemory();

    long then = System.currentTimeMillis();
    ServerProperties props = new ServerProperties(propsFile);
    String transactionsTable = props.getTransactionsTable();
    if (dbName == null) dbName = props.getSourceDatabase();
    if (dbName == null) {
      logger.error("please specify a source database via database=...");
      return;
    }
    MysqlConnection mysqlConnection = new MysqlConnection();
    String jdbcURL = mysqlConnection.getSimpleURL(dbName);//jdbc:mysql://localhost:3306/" + "test" + "?autoReconnect=true";
    BitcoinIngestUncharted bitcoinIngestUncharted = new BitcoinIngestUncharted();
    bitcoinIngestUncharted.doIngest(jdbcURL, transactionsTable, props.getFeatureDatabase(), writeDir,
        //skipLoadTransactions,
        limit, props);

    BitcoinFeaturesBase.logMemory();

    Runtime.getRuntime().gc();

    BitcoinFeaturesBase.logMemory();

    mysqlConnection.unregister();
    logger.info("ingest complete in " + (System.currentTimeMillis() - then) / 1000 + " seconds");
  }

  private static String getValue(String arg, String prefix) {
    return arg.split(prefix)[1];
  }

  private static long getLimit(long limit, String arg) {
    String prefix = "limit=";
    if (arg.startsWith(prefix)) {
      try {
        limit = Long.parseLong(getValue(arg, prefix));
      } catch (NumberFormatException e) {
        e.printStackTrace();
      }
    }
    return limit;
  }

  /**
   * @param dataSourceJDBC
   * @param transactionsTable
   * @param destinationDbName
   * @param writeDir
   * @param limit
   * @param props
   * @throws Throwable
   * @see #main(String[])
   */
  private void doIngest(String dataSourceJDBC, String transactionsTable,
                        String destinationDbName, String writeDir,
                        long limit,
                        ServerProperties props) throws Throwable {
    long start = System.currentTimeMillis();
    BitcoinFeaturesUncharted bitcoinFeaturesUncharted = new BitcoinFeaturesUncharted();

    Set<Long> userIds =
        loadTransactionsAndWriteFeatures(dataSourceJDBC, transactionsTable, destinationDbName,
            writeDir, limit, bitcoinFeaturesUncharted, props);

    long then2 = System.currentTimeMillis();
    Set<Long> validUsers = doSubgraphs(destinationDbName, userIds, props);
    logger.info("doIngest took " + (System.currentTimeMillis() - then2) / 1000 + " secs to do subgraphs");
    logger.info("doIngest took " + (System.currentTimeMillis() - start) / 1000 + " secs overall");

    boolean b = userIds.removeAll(validUsers);
    if (!userIds.isEmpty())
      logger.info("doIngest to remove " + userIds.size());

    H2Connection connection = bitcoinFeaturesUncharted.getConnection(destinationDbName);
    bitcoinFeaturesUncharted.pruneUsers(connection, userIds);
    connection.contextDestroyed();
  }

  /**
   * @param dataSourceJDBC
   * @param transactionsTable
   * @param destinationDbName
   * @param writeDir
   * @param limit
   * @param bitcoinFeaturesUncharted
   * @param props
   * @return
   * @throws Exception
   * @see #doIngest(String, String, String, String, long, ServerProperties)
   */
  private Set<Long> loadTransactionsAndWriteFeatures(String dataSourceJDBC,
                                                     String transactionsTable,
                                                     String destinationDbName,
                                                     String writeDir,
                                                     long limit,
                                                     BitcoinFeaturesUncharted bitcoinFeaturesUncharted,
                                                     ServerProperties props) throws Exception {
    long then = System.currentTimeMillis();
    // populate the transactions table
    MysqlInfo info = new MysqlInfo(props);
    info.setJdbc(dataSourceJDBC);
    info.setTable(transactionsTable);

    Map<Long, String> idToType = new HashMap<>();
    BitcoinIngestUnchartedTransactions bitcoinIngestUnchartedTransactions = new BitcoinIngestUnchartedTransactions(props);
    Collection<Long> users = bitcoinIngestUnchartedTransactions.getUsers(info, idToType);
    Map<Long, UserFeatures> idToStats = new HashMap<>();

//    if (!skipLoadTransactions) {
    logger.info("doIngest userIds size " + users.size() + " and " + idToStats.size());

    users = bitcoinIngestUnchartedTransactions.loadTransactionTable(info,
        "h2", destinationDbName, BitcoinBinding.TRANSACTIONS, USE_TIMESTAMP, limit, users, idToStats);

    logger.info("doIngest after userIds size " + users.size()
        //  + " includes " + users.contains(253479) + " or " + users.contains(12329212)
    );
    //  }

    // Extract features for each account
    new File(writeDir).mkdirs();

    Set<Long> userIds = bitcoinFeaturesUncharted.writeFeatures(destinationDbName, writeDir, users, idToStats, idToType);

    logger.info("doIngest userIds size " + userIds.size());

    long now = System.currentTimeMillis();
    logger.debug("doIngest Raw Ingest (loading transactions and extracting features) complete. Elapsed time: " +
        (now - then) / 1000 + " seconds");
    return userIds;
  }
}