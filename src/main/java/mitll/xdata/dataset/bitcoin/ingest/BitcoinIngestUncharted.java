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
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Performs all data ingest for Bitcoin data:
 * - Ingest raw transactions CSV
 * - Extract account features
 * - Build SubGraph Search Indices and Features
 */
public class BitcoinIngestUncharted extends BitcoinIngestBase {
  private static final Logger logger = Logger.getLogger(BitcoinIngestUncharted.class);

  private static final boolean USE_TIMESTAMP = false;

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
    String dataFilename = new MysqlConnection().getSimpleURL("bitcoin");//jdbc:mysql://localhost:3306/" + "test" + "?autoReconnect=true";
    if (args.length > 0) {
      dataFilename = args[0];
      logger.debug("got data file " + dataFilename);
    }

    String dbName = "bitcoin";
    if (args.length > 1) {
      dbName = args[1];
      logger.debug("got db name " + dbName);
    }

    String writeDir = "out";
    if (args.length > 2) {
      writeDir = args[2];
      logger.debug("got output dir " + writeDir);
    }

    new BitcoinIngestUncharted().doIngest(dataFilename, "usertransactions2013largerthandollar", dbName, writeDir);
  }

  private void doIngest(String dataSourceJDBC, String transactionsTable, String destinationDbName, String writeDir) throws Throwable {
    //
    // Raw Ingest (csv to database table + basic features)
    //
    long then = System.currentTimeMillis();

    Map<String, String> slotToCol = new HashMap<>();
    for (String col : Arrays.asList("TransactionId", "ReceiverId", "TxTime", "BTC", "USD")) {
      slotToCol.put(col, col);
    }

    // populate the transactions table
    int limit = 10000;
    new BitcoinIngestUnchartedTransactions().loadTransactionTable(dataSourceJDBC, transactionsTable,
        "h2", destinationDbName, BitcoinBinding.TRANSACTIONS, USE_TIMESTAMP, slotToCol, limit);

    // Extract features for each account
    new File(writeDir).mkdirs();

//		new BitcoinFeatures(destinationDbName, writeDir, dataFilename);

    long now = System.currentTimeMillis();
    logger.debug("Raw Ingest (loading transactions and extracting features) complete. Elapsed time: " + (now - then) / 1000 + " seconds");
    doSubgraphs(destinationDbName);
  }
}



