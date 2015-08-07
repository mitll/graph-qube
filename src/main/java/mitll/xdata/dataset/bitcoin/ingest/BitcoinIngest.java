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
import mitll.xdata.dataset.bitcoin.features.BitcoinFeatures;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;


/**
 * Performs all data ingest for Bitcoin data:
 * 	- Ingest raw transactions CSV
 *  - Extract account features
 *  - Build SubGraph Search Indices and Features
 */
public class BitcoinIngest extends BitcoinIngestBase {
	private static final Logger logger = Logger.getLogger(BitcoinIngest.class);

	private static final boolean USE_TIMESTAMP = false;
	private static final String BTC_TO_DOLLAR_CONVERSION_TXT = "btcToDollarConversion.txt";

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
		String dataFilename = "bitcoin-20130410.tsv";
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

  	new BitcoinIngest().doIngest(dataFilename, dbName, writeDir);
	}

	private void doIngest(String dataFilename, String dbName, String writeDir) throws Throwable {
		//
		// Raw Ingest (csv to database table + basic features)
		//
		long then = System.currentTimeMillis();

		// btc to Dollar Conversion
		String btcToDollarFile = "src/main/resources" +
				BitcoinBinding.BITCOIN_FEATS_TSV +
				BTC_TO_DOLLAR_CONVERSION_TXT;

		File file = new File(btcToDollarFile);
		if (!file.exists()) {
			logger.warn("can't find dollar conversion file " + file.getAbsolutePath());
		}
		logger.debug("BTC to Dollar File Loaded...");

		// populate the transactions table
		new BitcoinIngestRaw().loadTransactionTable(BitcoinBinding.TRANSACTIONS, dataFilename, btcToDollarFile, "h2", dbName, USE_TIMESTAMP);

		// Extract features for each account
		new File(writeDir).mkdirs();

		new BitcoinFeatures(dbName, writeDir, dataFilename);

		long now = System.currentTimeMillis();
		logger.debug("Raw Ingest (loading transactions and extracting features) complete. Elapsed time: " +(now-then)/1000 + " seconds");
		doSubgraphs(dbName);
	}

}



