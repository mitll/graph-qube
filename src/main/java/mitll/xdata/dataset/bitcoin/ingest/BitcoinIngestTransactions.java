package mitll.xdata.dataset.bitcoin.ingest;

import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Created by go22670 on 8/6/15.
 */
public class BitcoinIngestTransactions {
  private static final Logger logger = Logger.getLogger(BitcoinIngestRaw.class);
  IngestSql ingestSql = new IngestSql();

  private static UserStats getUserStats(Map<Integer, UserStats> userToStats, int sourceid) {
    UserStats userStats = userToStats.get(sourceid);
    if (userStats == null) userToStats.put(sourceid, userStats = new UserStats());
    return userStats;
  }

  protected double[] addAvgDollarFeatures(Map<Integer, UserStats> userToStats,
                                    double avgUSD, int count,
                                     int sourceid, int targetID, double usd) {
    double devFraction = (usd - avgUSD) / avgUSD;
    //    statement.setDouble(i++, devFraction);
    double[] addFeats = new double[3];
    addFeats[0] = devFraction;
    UserStats sourceStats = userToStats.get(sourceid);

    double avgCredit = sourceStats.getAvgCredit();
    double cdevFraction = avgCredit == 0 ? -1 : (usd - avgCredit) / avgCredit;
    addFeats[1] = cdevFraction;

    UserStats targetStats = userToStats.get(targetID);

    double avgDebit = targetStats.getAvgDebit();
    double ddevFraction = avgDebit == 0 ? -1 : (usd - avgDebit) / avgDebit;
    addFeats[2] = ddevFraction;

    if (count < 100) {
      logger.debug("source " + sourceid + " target " + targetID + " $" + usd + " avg " + avgUSD +
          " dev " + devFraction +
          " cavg  " + avgCredit +
          " cdev " + cdevFraction +
          " davg  " + avgDebit +
          " ddev " + ddevFraction);
    }
    return addFeats;
  }

  protected double addUserStats(Map<Integer, UserStats> userToStats, int sourceid, int targetID, double usd) {
    UserStats userStats = getUserStats(userToStats, sourceid);
    userStats.addDebit(usd);

    UserStats userStats2 = getUserStats(userToStats, targetID);
    userStats2.addCredit(usd);
    return usd;
  }
}
