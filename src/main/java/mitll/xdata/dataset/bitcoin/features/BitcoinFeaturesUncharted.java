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

import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/11/13
 * Time: 8:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class BitcoinFeaturesUncharted extends BitcoinFeaturesBase {
  private static final int HOUR_IN_MILLIS = 60 * 60 * 1000;
  private static final long DAY_IN_MILLIS = 24 * HOUR_IN_MILLIS;

  //private enum PERIOD {HOUR, DAY, WEEK, MONTH}
  //private final PERIOD period = PERIOD.DAY; // bin by day for now
  private static final Logger logger = Logger.getLogger(BitcoinFeaturesUncharted.class);
  public static final String ENTITYID = "entityid";
  public static final String FINENTITY = "FinEntity";

  /**
   * @param h2DatabaseFile
   * @param writeDirectory
   * @param info
   * @param limit
   * @throws Exception
   */
/*  public BitcoinFeaturesUncharted(String h2DatabaseFile, String writeDirectory, MysqlInfo info, long limit,
                                  Collection<Integer> users) throws Exception {
    this(new H2Connection(h2DatabaseFile, 38000000), writeDirectory, info, false, limit, users);
  }*/

  public Set<Integer> writeFeatures(String h2DatabaseFile, String writeDirectory, MysqlInfo info, long limit,
                                  Collection<Integer> users) throws Exception {
    return writeFeatures(new H2Connection(h2DatabaseFile, 38000000), writeDirectory, info, false, limit, users);
  }

  /**
   * # normalize features
   * m, v = mean(bkgFeatures, 2), std(bkgFeatures, 2)
   * mnormedFeatures  = accountFeatures - hcat([m for i = 1:size(accountFeatures, 2)]...)
   * mvnormedFeatures = mnormedFeatures ./ hcat([v for i = 1:size(accountFeatures, 2)]...)
   * weights          = [ specWeight * ones(length(m) - 10);
   * statWeight * ones(2);
   * iarrWeight * ones(2);
   * statWeight * ones(2);
   * iarrWeight * ones(2);
   * (args["graph"] ? ppWeight : 0.0) * ones(2)
   * ]
   * weightedmv       = mvnormedFeatures .* weights
   * <p>
   * Writes out four files -- pairs.txt, bitcoin_features.tsv, bitcoin_raw_features.tsv, and bitcoin_ids.tsv
   *
   * @param connection
   * @throws Exception
   * @paramz datafile   original flat file of data - transactions!
   * @see #main(String[])
   */
  private Set<Integer> writeFeatures(DBConnection connection, String writeDirectory, MysqlInfo info,
                                   boolean useSpectralFeatures, long limit,
                                   Collection<Integer> users) throws Exception {
    long then = System.currentTimeMillis();
    // this.useSpectral = useSpectralFeatures;
    // long now = System.currentTimeMillis();
    // logger.debug("took " +(now-then) + " to read " + transactions);
    logger.debug("reading users from db " + connection + " users " + users.size());

    //Collection<Integer> users = getUsers(connection);

    String pairsFilename = writeDirectory + "pairs.txt";

    writePairs(users, info, pairsFilename, limit);

    Map<Integer, UserFeatures> transForUsers = getTransForUsers(info, users, limit);

    return writeFeatures(connection, writeDirectory, then, users, transForUsers);
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
  Collection<Integer> getUsers(DBConnection connection) throws Exception {
    long then = System.currentTimeMillis();

	  /*
     * Execute updates to figure out
	   */
    String filterUsers = "select " +
        ENTITYID +
        " from " +
        FINENTITY +
        " where numtransactions > " + MIN_TRANSACTIONS;

    PreparedStatement statement = connection.getConnection().prepareStatement(filterUsers);
    statement.executeUpdate();
    ResultSet rs = statement.executeQuery();

    Set<Integer> ids = new HashSet<Integer>();
    int c = 0;

    while (rs.next()) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " + c);
      ids.add(rs.getInt(1));
    }
    long now = System.currentTimeMillis();
    logger.debug("getUsers took " + (now - then) + " millis to read " + ids.size() + " users");

    rs.close();
    statement.close();
    return ids;
  }

  /**
   * The binding reads the file produced here to support connected lookup.
   *
   * @param users
   * @param dataFilename
   * @param outfile
   * @throws Exception
   * @see BitcoinBinding#populateInMemoryAdjacency()
   * @see #BitcoinFeaturesUncharted(DBConnection, String, MysqlInfo, boolean, long, Collection)
   */
  private void writePairs(Collection<Integer> users,
                          MysqlInfo info,
                          String outfile,
                          long limit) throws Exception {
    int count = 0;
    long t0 = System.currentTimeMillis();
    //int c = 0;
    Map<Integer, Set<Integer>> stot = new HashMap<Integer, Set<Integer>>();
    int skipped = 0;

    Connection uncharted = new MysqlConnection().connectWithURL(info.getJdbc());

    logMemory();

    String sql = "select " +
        info.getSlotToCol().get(MysqlInfo.SENDER_ID) + ", " +
        info.getSlotToCol().get(MysqlInfo.RECEIVER_ID) +
        " from " + info.getTable() + " limit " + limit;

    logger.debug("writePairs exec " + sql);
    PreparedStatement statement = uncharted.prepareStatement(sql);
    statement.setFetchSize(1000000);
    logMemory();

//    logger.debug("writePairs Getting result set --- ");
    ResultSet resultSet = statement.executeQuery();
//    logger.debug("writePairs Got     result set --- ");

    int mod = 1000000;

    logMemory();

    while (resultSet.next()) {
      count++;

      int col = 1;
      int source = resultSet.getInt(col++);
      int target = resultSet.getInt(col++);

      if (getSkipped(users, stot, source, target)) skipped++;

      if (count % mod == 0) {
        logger.debug("writePairs transaction count = " + count + "; " + (System.currentTimeMillis() - t0) / count + " ms/read");
        logMemory();
      }
    }

    resultSet.close();
    uncharted.close();

    logger.debug("writePairs Got past result set - wrote " + count + " skipped " + skipped );

    //for (Long pair : connectedPairs) writer.write(pair+"\n");
    writePairs(outfile, stot);
  }

  /**
   * Store two integers in a long.
   * <p>
   * longs are 64 bits, store the low int in the low 32 bits, and the high int in the upper 32 bits.
   *
   * @param low  this is converted from an integer
   * @param high
   * @return
   * @see
   */
/*  private long storeTwo(long low, long high) {
    long combined = low;
    combined += high << 32;
    return combined;
  }*/

  /**
   * @param dataFilename
   * @param users        transactions must be between the subset of non-trivial users (who have more than 10 transactions)
   * @return
   * @throws Exception
   * @see #BitcoinFeatures(DBConnection, String, String, boolean)
   */
  protected Map<Integer, UserFeatures> getTransForUsers(MysqlInfo info, Collection<Integer> users, long limit) throws Exception {
    int count = 0;
    long t0 = System.currentTimeMillis();

    Map<Integer, UserFeatures> idToStats = new HashMap<Integer, UserFeatures>();

    logMemory();

    String sql = "select " +
        info.getSlotToCol().get(MysqlInfo.SENDER_ID) + ", " +
        info.getSlotToCol().get(MysqlInfo.RECEIVER_ID) + ", " +
        info.getSlotToCol().get(MysqlInfo.TX_TIME) + ", " +
        info.getSlotToCol().get(MysqlInfo.USD) + //", " +
        " from " + info.getTable() + " limit " + limit;

    logger.debug("getTransForUsers exec " + sql);
    Connection uncharted = new MysqlConnection().connectWithURL(info.getJdbc());

    PreparedStatement statement = uncharted.prepareStatement(sql);
    statement.setFetchSize(1000000);
    logMemory();

//    logger.debug("getTransForUsers Getting result set --- ");
    ResultSet resultSet = statement.executeQuery();
  //  logger.debug("getTransForUsers Got     result set --- ");

    int mod = 1000000;

    logMemory();

    int skipped = 0;
    while (resultSet.next()) {
      count++;

      int col = 1;
      int source = resultSet.getInt(col++);
      int target = resultSet.getInt(col++);

      //     boolean onlyGetDailyData = period == PERIOD.DAY;
      //  Calendar calendar = Calendar.getInstance();
      if (users.contains(source) && users.contains(target)) {
        long time1 = resultSet.getTimestamp(col++).getTime();
        long time = (time1 / DAY_IN_MILLIS) * DAY_IN_MILLIS;

        double amount = resultSet.getDouble(col++);

        addTransaction(idToStats, source, target, time, amount);
      }
      else {
        skipped++;
      }
      if (count % 1000000 == 0) {
        logger.debug("read " + count + " transactions... " + (System.currentTimeMillis() - 1.0 * t0) / count + " ms/read");
      }
    }
    logger.info("getTransForUsers skipped " + skipped + " out of " + count + " -> " + idToStats.size());

    return idToStats;
  }
}
