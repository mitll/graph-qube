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

import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/11/13
 * Time: 8:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class BitcoinFeaturesUncharted extends BitcoinFeaturesBase {
  //  private static final int HOUR_IN_MILLIS = 60 * 60 * 1000;
//  private static final long DAY_IN_MILLIS = 24 * HOUR_IN_MILLIS;

  private static final Logger logger = Logger.getLogger(BitcoinFeaturesUncharted.class);
  private static final String ENTITYID = "entityid";
  private static final String FINENTITY = "FinEntity";
  private static final int MAX_MEMORY_ROWS = 38000000;

  /**
   * @param h2DatabaseFile
   * @param writeDirectory
   * @throws Exception
   * @paramx info
   * @paramx limit
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestUncharted#doIngest
   */
  public Set<Long> writeFeatures(String h2DatabaseFile, String writeDirectory, //MysqlInfo info, long limit,
                                 Collection<Long> users,
                                 Map<Long, UserFeatures> idToFeatures
  ) throws Exception {
    H2Connection connection = getConnection(h2DatabaseFile);
    Set<Long> longs = writeFeatures(connection, writeDirectory, //info, limit,
        users, idToFeatures);
//    connection.contextDestroyed();
    return longs;
  }

  /**
   *
   * @param h2DatabaseFile
   * @return
   */
  public H2Connection getConnection(String h2DatabaseFile) {
    return new H2Connection(h2DatabaseFile, MAX_MEMORY_ROWS);
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
   * @seex #main(String[])
   * @see #writeFeatures(String, String, MysqlInfo, long, Collection)
   */
  private Set<Long> writeFeatures(DBConnection connection, String writeDirectory,
                                  //MysqlInfo info,
                                  //long limit,
                                  Collection<Long> users,
                                  Map<Long, UserFeatures> transForUsers) throws Exception {
    long then = System.currentTimeMillis();
    // long now = System.currentTimeMillis();
    // logger.debug("took " +(now-then) + " to read " + transactions);
    logger.debug("writeFeatures reading users from db " + connection + " users " + users.size());

    //  Map<Integer, UserFeatures> transForUsers = getTransForUsers(info, users, limit);

    return writeFeatures(connection, writeDirectory, then, users, transForUsers);
  }

  /**
   * Read from uncharted bitcoin database table : {@link #FINENTITY}
   * <p>
   * Filter out accounts that have less than {@link #MIN_TRANSACTIONS} transactions.
   * NOTE : throws out "supernode" #25
   *
   * @param connection
   * @return
   * @throws Exception
   * @see BitcoinFeatures#BitcoinFeatures
   */
  Collection<Long> getUsers(DBConnection connection) throws Exception {
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

    Set<Long> ids = new HashSet<>();
    int c = 0;

    while (rs.next()) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " + c);
      ids.add(rs.getLong(1));
    }
    long now = System.currentTimeMillis();
    logger.debug("getUsers took " + (now - then) + " millis to read " + ids.size() + " users");

    rs.close();
    statement.close();
    return ids;
  }

  /**
   * @param info
   * @param users transactions must be between the subset of non-trivial users (who have more than 10 transactions)
   * @return user id->User Features map
   * @throws Exception
   * @see #writeFeatures(DBConnection, String, MysqlInfo, long, Collection)
   */
/*  private Map<Integer, UserFeatures> getTransForUsers(MysqlInfo info, Collection<Integer> users, long limit)
      throws Exception {
    int count = 0;
    long t0 = System.currentTimeMillis();

    Map<Integer, UserFeatures> idToStats = new HashMap<>();

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

    // int mod = 1000000;

    logMemory();

    int skipped = 0;
    while (resultSet.next()) {
      count++;

      int col = 1;
      int source = resultSet.getInt(col++);
      int target = resultSet.getInt(col++);

      //     boolean onlyGetDailyData = period == PERIOD.DAY;
      if (users.contains(source) && users.contains(target)) {
        long day = roundToDay(resultSet.getTimestamp(col++).getTime());
        double amount = resultSet.getDouble(col++);

        addTransaction(idToStats, source, target, day, amount);
      } else {
        skipped++;
      }
      if (count % 1000000 == 0) {
        double diff = System.currentTimeMillis() - t0;
        if (diff > 1000) diff /= 1000;
        logger.debug("getTransForUsers read " + count + " transactions... " + count / diff + " read/sec");
        logMemory();
      }
    }
    logger.info("getTransForUsers skipped " + skipped + " out of " + count + " -> " + idToStats.size());
    logMemory();

    resultSet.close();
    statement.close();

    return idToStats;
  }

  private long roundToDay(long time1) {
    return (time1 / DAY_IN_MILLIS) * DAY_IN_MILLIS;
  }*/
}
