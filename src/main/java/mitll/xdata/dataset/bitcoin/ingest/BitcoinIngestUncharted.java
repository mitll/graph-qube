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

import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesUncharted;
import mitll.xdata.dataset.bitcoin.features.MysqlInfo;
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Collection;


/**
 * Performs all data ingest for Bitcoin data:
 * - Ingest raw transactions CSV
 * - Extract account features
 * - Build SubGraph Search Indices and Features
 */
public class BitcoinIngestUncharted extends BitcoinIngestBase {
  private static final Logger logger = Logger.getLogger(BitcoinIngestUncharted.class);

  private static final boolean USE_TIMESTAMP = false;
  public static final String BITCOIN = "bitcoin";
  public static final String USERTRANSACTIONS_2013_LARGERTHANDOLLAR = "usertransactions2013largerthandollar";
  public static final String SKIP_TRUE = "skip=true";

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
    logger.debug("Starting Ingest...");

    //
    // Parse Arguments...
    //
    String dataFilename = new MysqlConnection().getSimpleURL(BITCOIN);//jdbc:mysql://localhost:3306/" + "test" + "?autoReconnect=true";
    boolean skipLoadTransactions = false;
    if (args.length > 0) {
      String first = args[0];
      if (first.startsWith("skip=")) {
        skipLoadTransactions = first.equals(SKIP_TRUE);
      }
      else {
        logger.debug("got data file " + dataFilename);
        dataFilename = first;
      }
    }

    String dbName = "bitcoin";
    if (args.length > 1) {
      String second = args[1];
      if (!second.contains("=")) {
        dbName = second;
        logger.debug("got db name '" + dbName + "'");
      }
    }

    String writeDir = "outUncharted";
    if (args.length > 2) {
      writeDir = args[2];
      logger.debug("got output dir " + writeDir);
    }

    if (args.length > 3) {
      skipLoadTransactions = args[3].equals("skip");
      logger.debug("got skip load transactions " + skipLoadTransactions);
    }
//    long limit = 20000000000l;
    long limit = 20000l;
    for (String arg : args) {
      if (arg.startsWith("limit=")) {
        try {
          limit = Long.parseLong(arg.split("limit=")[1]);
        } catch (NumberFormatException e) {
          e.printStackTrace();
        }
      }
    }

    new BitcoinIngestUncharted().doIngest(dataFilename, USERTRANSACTIONS_2013_LARGERTHANDOLLAR, dbName, writeDir,
        skipLoadTransactions, limit);
  }

  private void doIngest(String dataSourceJDBC, String transactionsTable, String destinationDbName, String writeDir,
                        boolean skipLoadTransactions, long limit) throws Throwable {
    //
    // Raw Ingest (csv to database table + basic features)
    //
    long then = System.currentTimeMillis();

    // populate the transactions table
   // int limit = 1000000;
    MysqlInfo info = new MysqlInfo();
    info.setJdbc(dataSourceJDBC);
    info.setTable(transactionsTable);

    if (!skipLoadTransactions) {
      new BitcoinIngestUnchartedTransactions().loadTransactionTable(info,
          "h2", destinationDbName, BitcoinBinding.TRANSACTIONS, USE_TIMESTAMP, limit);
    }

    Collection<Integer> users = new BitcoinIngestUnchartedTransactions().getUsers(info);

    // Extract features for each account
    new File(writeDir).mkdirs();

   // int limit1 = 1000000;
    new BitcoinFeaturesUncharted(destinationDbName, writeDir, info, limit, users);

    long now = System.currentTimeMillis();
    logger.debug("Raw Ingest (loading transactions and extracting features) complete. Elapsed time: " + (now - then) / 1000 + " seconds");
    doSubgraphs(destinationDbName);
  }
}



