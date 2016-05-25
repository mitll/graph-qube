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

import edu.emory.mathcs.jtransforms.fft.FloatFFT_1D;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/11/13
 * Time: 8:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class BitcoinFeatures extends BitcoinFeaturesBase {
//  private static final int MIN_TRANSACTIONS = 10;
//  private static final boolean LIMIT = false;
//  private static final int BITCOIN_OUTLIER = 25;
//  private static final int USER_LIMIT = 10000000;
//  private static final int MIN_DEBITS = 5;
//  private static final int MIN_CREDITS = 5;
//  private static final List<Double> EMPTY_DOUBLES = Arrays.asList(0d, 0d);
  private static final int SPECLEN = 100;//
//  private static final int NUM_STANDARD_FEATURES = 10;
  //public static final String BITCOIN_IDS_TSV = "bitcoin_ids.tsv";
//  private static final String BITCOIN_RAW_FEATURES_TSV = "bitcoin_raw_features.tsv";
  public static final String BITCOIN_FEATURES_STANDARDIZED_TSV = "bitcoin_features_standardized.tsv";
  // private static final boolean USE_SPECTRAL_FEATURES = true;
  // double specWeight = 1.0;
  // private final double statWeight = 15.0;
  // private final double iarrWeight = 30.0;
  // private final double ppWeight   = 20.0;
 // private boolean useSpectral = false;

 // private static final int HOUR_IN_MILLIS = 60 * 60 * 1000;

  /**
   * Populate map so we can tell when to skip transactions from users who have done < 25 transactions.
   *
   * @param users
   * @param stot
   * @paramx skipped
   * @param source
   * @param target
   * @return
   */
  private boolean getSkipped(Collection<Long> users, Map<Long, Set<Long>> stot, long source, long target) {
    if (users.contains(source) && users.contains(target)) {
      Set<Long> integers = stot.get(source);
      if (integers == null) stot.put(source, integers = new HashSet<>());
      if (!integers.contains(target)) integers.add(target);
      return false;
    } else {
      return true;
    }
//    return skipped;
  }

  private void writePairs(String outfile, Map<Long, Set<Long>> stot) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(outfile));
    int cc = 0;
    for (Map.Entry<Long, Set<Long>> pair : stot.entrySet()) {
      for (Long i : pair.getValue()) {
        writer.write(storeTwo(pair.getKey(), i) + "\n");
        cc++;
      }
    }
    writer.close();

    logger.debug("writePairs wrote " + cc + " pairs to " + outfile);

    Runtime.getRuntime().gc();
  }
  // private static final long DAY_IN_MILLIS = 24 * HOUR_IN_MILLIS;

  private enum PERIOD {HOUR, DAY, WEEK, MONTH}

  private final PERIOD period = PERIOD.DAY; // bin by day for now
  private static final Logger logger = Logger.getLogger(BitcoinFeatures.class);


  public BitcoinFeatures(String h2DatabaseFile, String writeDirectory, String datafile) throws Exception {
    this(new H2Connection(h2DatabaseFile, 38000000), writeDirectory, datafile, false);
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
   * @param datafile   original flat file of data - transactions!
   * @throws Exception
   * @see #main(String[])
   */
  private BitcoinFeatures(DBConnection connection, String writeDirectory, String datafile, boolean useSpectralFeatures) throws Exception {
    long then = System.currentTimeMillis();
  //  this.useSpectral = useSpectralFeatures;
    // long now = System.currentTimeMillis();
    // logger.debug("took " +(now-then) + " to read " + transactions);
    logger.debug("reading users from db " + connection);

    Collection<Long> users = getUsers(connection);

    String pairsFilename = writeDirectory + "pairs.txt";

    writePairs(users, datafile, pairsFilename);

    Map<Long, UserFeatures> transForUsers = getTransForUsers(datafile, users);

    writeFeatures(connection, writeDirectory, then, users, transForUsers);
  }

  /**
   * TODO : accumulate 158/160 dim feature vector:
   *
   *    name         = $nms
   credit_spec  = $credit_spec    x50
   debit_spec   = $debit_spec     x50
   merge_spec   = $merge_spec     x50
   credit_stats = $credit_stats
   credit_iarr  = $credit_iarr
   debit_stats  = $debit_stats
   debit_iarr   = $debit_iarr
   p_in         = $p_in           perp
   p_out        = $p_out          perp
   * @param transactions
   * @deprecated
   */
/*  private void getStats(List<Transaction> transactions) {
    long min = 0;
    long max = 0;
    Map<Integer,Integer> idToIn = new HashMap<Integer, Integer>();
    Map<Integer,Integer> idToOut = new HashMap<Integer, Integer>();

    for (Transaction t : transactions) {
      if (t.time < min) min = t.time;
      if (t.time > max) max = t.time;

      Integer out = idToOut.get(t.source);
      idToOut.put(t.source, out == null ? 1 : out+1);

      Integer in  = idToIn.get(t.target);
      idToIn.put(t.target,  in == null  ? 1 : in+1);
    }

    logger.debug("min time " + new Date(min) + " max " + new Date(max));
  }*/

  /**
   * TODO : bitcoin too big to fit into memory... :(
   *
   *
   * @param connection
   * @return
   * @throws Exception
   * @deprecated
   */
/*  private List<Transaction> getTransactions(DBConnection connection) throws Exception {
      String sql = "select SOURCE,TARGET,TIME,AMOUNT from TRANSACTIONS where SOURCE <> 25 AND TARGET <> 25";

      PreparedStatement statement = connection.getConnection().prepareStatement(sql);
      ResultSet rs = statement.executeQuery();
      List<Transaction> transactions = new ArrayList<Transaction>();
      List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
      int c = 0;

      while (rs.next()) {
        c++;
        if (c % 100000 == 0) logger.debug("read  " +c);
        Transaction e = new Transaction(rs);
        transactions.add(e);
      }

      if (false) {
        logger.debug("Got " + rows.size() + " ");
        for (Map<String, String> row : rows) logger.debug(row);
      }
      rs.close();
      statement.close();
      return  transactions;
  }*/

  /**
   * The binding reads the file produced here to support connected lookup.
   *
   * @param users
   * @param dataFilename
   * @param outfile
   * @throws Exception
   * @see mitll.xdata.dataset.bitcoin.binding.BitcoinBinding#populateInMemoryAdjacency()
   * @see #BitcoinFeatures(mitll.xdata.db.DBConnection, String, String, boolean)
   */
  private void writePairs(Collection<Long> users,
                          String dataFilename, String outfile) throws Exception {

    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
    String line;
    int count = 0;
    long t0 = System.currentTimeMillis();
    int max = Integer.MAX_VALUE;
    int bad = 0;

    //  Map<Integer,UserFeatures> idToStats = new HashMap<Integer,UserFeatures>();
    //  Set<Long> connectedPairs = new HashSet<Long>(10000000);
    int c = 0;
    Map<Long, Set<Long>> stot = new HashMap<>();
    int skipped = 0;
    while ((line = br.readLine()) != null) {
      count++;
      if (count > max) break;
      String[] split = line.split("\\s+"); // 4534248 25      25      2013-01-27 22:41:38     9.91897304
      if (split.length != 6) {
        bad++;
        if (bad < 10) logger.warn("badly formed line " + line);
      }

      int source = Integer.parseInt(split[1]);
      int target = Integer.parseInt(split[2]);

      if (getSkipped(users, stot, source, target)) skipped++;
      // long key = storeTwo(source, target);
      //if (!connectedPairs.contains(key)) connectedPairs.add(key);
      if (c++ % 1000000 == 0) logger.debug("read " + c + " from " + dataFilename);

      if (count % 10000000 == 0) {
        logger.debug("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count
            + " ms/read");
      }
    }
    logger.debug("skipped " + skipped + " transactions where either the source or target has been pruned");
    br.close();

    //for (Long pair : connectedPairs) writer.write(pair+"\n");
    writePairs(outfile, stot);
    if (bad > 0) logger.warn("Got " + bad + " transactions...");
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
   Map<Long, UserFeatures> getTransForUsers(String dataFilename, Collection<Long> users) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
    String line;
    int count = 0;
    long t0 = System.currentTimeMillis();
    int max = Integer.MAX_VALUE;
    int bad = 0;

    Map<Long, UserFeatures> idToStats = new HashMap<>();

    while ((line = br.readLine()) != null) {
      count++;
      if (count > max) break;
      String[] split = line.split("\\s+"); // 4534248 25      25      2013-01-27 22:41:38     9.91897304
      if (split.length != 6) {
        bad++;
        if (bad < 10) logger.warn("badly formed line " + line);
      }

      int source = Integer.parseInt(split[1]);
      int target = Integer.parseInt(split[2]);

      boolean onlyGetDailyData = period == PERIOD.DAY;
      Calendar calendar = Calendar.getInstance();
      if (users.contains(source) && users.contains(target)) {
        long time = getTime(split, onlyGetDailyData, calendar);
        double amount = Double.parseDouble(split[5]);
        addTransaction(idToStats, source, target, time, amount);
      }
      if (count % 1000000 == 0) {
        logger.debug("read " + count + " transactions... " + (System.currentTimeMillis() - 1.0 * t0) / count + " ms/read");
      }
    }
    if (bad > 0) logger.warn("Got " + bad + " transactions...");
    return idToStats;
  }

  private long getTime(String[] split, boolean onlyGetDailyData, Calendar calendar) {
    long time;
    String yyyyMMdd = split[3];
    if (onlyGetDailyData) {
      getDay(calendar, yyyyMMdd);
    } else {
      getDayAndTime(calendar, yyyyMMdd, split[4]);
    }
    time = calendar.getTimeInMillis();
    return time;
  }

  private void getDay(Calendar calendar, String yyyyMMdd) {
    String year = yyyyMMdd.substring(0, 4);
    String month = yyyyMMdd.substring(5, 7);
    String day = yyyyMMdd.substring(8, 10);
    // logger.debug("value " +year + " " + month + " " + day);
    int imonth = Integer.parseInt(month) - 1;
    calendar.set(Integer.parseInt(year), imonth, Integer.parseInt(day));
  }

  private void getDayAndTime(Calendar calendar, String yyyyMMdd, String hhmmss) {
    String year = yyyyMMdd.substring(0, 4);
    String month = yyyyMMdd.substring(5, 7);
    String day = yyyyMMdd.substring(8, 10);
    // logger.debug("value " +year + " " + month + " " + day);
    int imonth = Integer.parseInt(month) - 1;

  /*  String hour = hhmmss.substring(0, 2);
    String min = hhmmss.substring(3, 5);
    String sec = hhmmss.substring(6, 8);*/

    // logger.debug("value " +year + " " + month + " " + day + " " + hour + " " + min + " " + sec);

    calendar.set(Integer.parseInt(year), imonth, Integer.parseInt(day));
  }

  /**
   * TODO : fill this in -- note we want only the real components:
   * <p>
   * Julia from Wade:
   * function spectrum(timeLine)
   * data = timeLine["total"].data
   * r = zeros(int(speclen/2) + 1)
   * n = 0.0
   * for i = 1:int(speclen/2):length(data)
   * ds = (i + speclen > length(data)) ? [ data[i:length(data)]; zeros(speclen - (length(data) - i)) ] : data[i:(i+speclen)]
   * r += abs(rfft(ds))
   * n += 1
   * end
   * fdata = r / n
   * fstep = 0.5 / (length(fdata) - 1)
   * <p>
   * tfreq = DataFrame()
   * tfreq["power"]  = fdata[2:end]
   * <p>
   * return tfreq
   * end
   */

  private float[] getSpectrum(float[] signal) {
    return getSpectrum(signal, SPECLEN);
  }

  /**
   * Skip first real component, since just the sum of the inputs
   * <p>
   * TODO : we always have a zero at the end of the results -- seems like we always have n-1 fft values after
   * throwing away DC component.
   *
   * @param signal
   * @param speclen
   * @return
   */
  private float[] getSpectrum(float[] signal, int speclen) {
    FloatFFT_1D fft = new FloatFFT_1D(speclen);
    int half = speclen / 2;
    float[] tmp = new float[speclen];
    float[] r = new float[half];
    float n = 0;

    for (int i = 0; i < signal.length; i += half) {
      int length = speclen;
      if (i + length > signal.length) {
        Arrays.fill(tmp, 0f);
        length = signal.length - i;
        //  logger.debug("at end " + i + " len " + length + " vs " + signal.length);
      }
      System.arraycopy(signal, i, tmp, 0, length);
      //for (int x = 0; x < tmp.length; x++) logger.debug(x + " : " + tmp[x]);
      fft.realForward(tmp);
      // int k = 0;
      for (int j = 1; j < half; j++) {
        int i1 = j * 2;
        //   logger.debug("at " + i1);
        float realComponent = tmp[i1];
        r[j - 1] = Math.abs(realComponent);
      }
      n += 1;
    }
    for (int i = 0; i < r.length; i++) r[i] /= n;

    return r;
  }


  @SuppressWarnings("CanBeFinal")
  private static class Transaction implements Comparable<Transaction> {
    final int source;
    final int target;
    final long time;
    final double amount;
/*    public Transaction(ResultSet rs) throws Exception {
      int i = 1;
      source = rs.getInt(i++);
      target = rs.getInt(i++);
      //time = rs.getTimestamp(i++).getTime();
      time = rs.getLong(i++);
      amount = rs.getDouble(i);
    }*/

    public Transaction(int source, int target, long time, double amount) {
      this.source = source;
      this.target = target;
      this.time = time;
      this.amount = amount;
    }

    @Override
    public int compareTo(Transaction o) {
      return
          time < o.time ? -1 : time > o.time ? +1 :
              amount < o.amount ? -1 : amount > o.amount ? +1 :
                  target < o.target ? -1 : target > o.target ? +1 : 0;
    }
  }

  public static void main(String[] args) {
    try {
      String dataFilename = null;
      //boolean useSpectralFeatures = false;
      if (args.length > 0) {
        dataFilename = args[0];
        logger.debug("got " + dataFilename);
      }
      //if (args.length > 1) {
      //  useSpectralFeatures = args[1].toLowerCase().contains("spectral");
      //}
      long then = System.currentTimeMillis();

      String database = "bitcoin";
      new BitcoinFeatures(database, ".", dataFilename);

      long now = System.currentTimeMillis();
      logger.debug("took " + (now - then) + " millis to generate features");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
