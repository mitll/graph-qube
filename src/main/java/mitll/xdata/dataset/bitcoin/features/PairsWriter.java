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
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by go22670 on 2/17/16.
 */
public class PairsWriter {
  private static final Logger logger = Logger.getLogger(PairsWriter.class);

  /**
   * The binding reads the file produced here to support connected lookup.
   *
   * @param users
   * @param info
   * @param outfile
   * @throws Exception
   * @see BitcoinBinding#populateInMemoryAdjacency()
   * @see #writeFeatures(DBConnection, String, MysqlInfo, boolean, long, Collection)
   */
  public void writePairs(Collection<Integer> users,
                          MysqlInfo info,
                          String outfile,
                          long limit) throws Exception {
    int count = 0;
    long t0 = System.currentTimeMillis();
    //int c = 0;
    Map<Integer, Set<Integer>> stot = new HashMap<Integer, Set<Integer>>();
    int skipped = 0;

    Connection uncharted = new MysqlConnection().connectWithURL(info.getJdbc());

    BitcoinFeaturesBase.logMemory();

    String sql = "select " +
        info.getSlotToCol().get(MysqlInfo.SENDER_ID) + ", " +
        info.getSlotToCol().get(MysqlInfo.RECEIVER_ID) +
        " from " + info.getTable() + " limit " + limit;

    logger.debug("writePairs exec " + sql);
    PreparedStatement statement = uncharted.prepareStatement(sql);
    statement.setFetchSize(1000000);
    BitcoinFeaturesBase.logMemory();

//    logger.debug("writePairs Getting result set --- ");
    ResultSet resultSet = statement.executeQuery();
//    logger.debug("writePairs Got     result set --- ");

    int mod = 1000000;

    BitcoinFeaturesBase.logMemory();

    while (resultSet.next()) {
      count++;

      int col = 1;
      int source = resultSet.getInt(col++);
      int target = resultSet.getInt(col++);

      if (getSkipped(users, stot, source, target)) skipped++;

      if (count % mod == 0) {
        logger.debug("writePairs transaction count = " + count + "; " + (System.currentTimeMillis() - t0) / count + " ms/read");
        BitcoinFeaturesBase.logMemory();
      }
    }

    resultSet.close();
    uncharted.close();

    logger.debug("writePairs Got past result set - wrote " + count + " skipped " + skipped);

    //for (Long pair : connectedPairs) writer.write(pair+"\n");
    writePairs(outfile, stot);
  }


  /**
   * Populate map so we can tell when to skip transactions from users who have done < 25 transactions.
   *
   * @param users
   * @param stot
   * @param skipped
   * @param source
   * @param target
   * @return
   */
  protected boolean getSkipped(Collection<Integer> users, Map<Integer, Set<Integer>> stot, int source, int target) {
    if (users.contains(source) && users.contains(target)) {
      Set<Integer> integers = stot.get(source);
      if (integers == null) stot.put(source, integers = new HashSet<Integer>());
      if (!integers.contains(target)) integers.add(target);
      return false;
    } else {
      return true;
    }
//    return skipped;
  }

  protected void writePairs(String outfile, Map<Integer, Set<Integer>> stot) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(outfile));
    int cc = 0;
    for (Map.Entry<Integer, Set<Integer>> pair : stot.entrySet()) {
      for (Integer i : pair.getValue()) {
        writer.write(storeTwo(pair.getKey(), i) + "\n");
        cc++;
      }
    }
    writer.close();

    logger.debug("writePairs wrote " + cc + " pairs to " + outfile);

    Runtime.getRuntime().gc();
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
  public static long storeTwo(long low, long high) {
//    long combined = low;
//    combined += high << 32;
//    return combined;

    long l = (high << 32) | (low & 0xffffffffL);
    return l;
  }


}
