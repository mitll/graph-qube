package mitll.xdata.dataset.bitcoin.features;

import edu.emory.mathcs.jtransforms.fft.FloatFFT_1D;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/11/13
 * Time: 8:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class BitcoinFeatures {
  private static final int MIN_TRANSACTIONS = 10;
  private static final boolean LIMIT = false;
  private static final int BITCOIN_OUTLIER = 25;
  private static final int USER_LIMIT = 10000000;
  private static final int MIN_DEBITS = 5;
  private static final int MIN_CREDITS = 5;
  private static final List<Double> EMPTY_DOUBLES = Arrays.asList(0d, 0d);
  private static final int SPECLEN = 100;//
 // private static final boolean USE_SPECTRAL_FEATURES = true;
  double specWeight = 1.0;
  private final double statWeight = 15.0;
  private final double iarrWeight = 30.0;
  private final double ppWeight   = 20.0;
  private boolean useSpectral = false;

  private static final int HOUR_IN_MILLIS = 60 * 60 * 1000;
  private static final long DAY_IN_MILLIS = 24 * HOUR_IN_MILLIS;
  private enum PERIOD { HOUR, DAY, WEEK, MONTH }
  private final PERIOD period = PERIOD.DAY; // bin by day for now
  private static final Logger logger = Logger.getLogger(BitcoinFeatures.class);

/*
  private long getTimeForDay(Calendar calendar, String yyyyMMdd) {
    getDay(calendar, yyyyMMdd);
    return calendar.getTimeInMillis();
  }
*/

  /**
   *   # normalize features
   m, v = mean(bkgFeatures, 2), std(bkgFeatures, 2)
   mnormedFeatures  = accountFeatures - hcat([m for i = 1:size(accountFeatures, 2)]...)
   mvnormedFeatures = mnormedFeatures ./ hcat([v for i = 1:size(accountFeatures, 2)]...)
   weights          = [ specWeight * ones(length(m) - 10);
   statWeight * ones(2);
   iarrWeight * ones(2);
   statWeight * ones(2);
   iarrWeight * ones(2);
   (args["graph"] ? ppWeight : 0.0) * ones(2)
   ]
   weightedmv       = mvnormedFeatures .* weights
   * @param connection
   * @param datafile
   * @throws Exception
   */
  private BitcoinFeatures(DBConnection connection, String datafile, boolean useSpectralFeatures) throws Exception {
     long then = System.currentTimeMillis();
    this.useSpectral = useSpectralFeatures;
   // long now = System.currentTimeMillis();
   // logger.debug("took " +(now-then) + " to read " + transactions);

    Collection<Integer> users = getUsers(connection);
    writePairs(users, datafile, "pairs.txt");

    Map<Integer, UserFeatures> transForUsers = getTransForUsers(datafile, users);
    long now = System.currentTimeMillis();
    logger.debug("took " +(now-then) + " to read " + transForUsers.size());

    BufferedWriter writer = new BufferedWriter(new FileWriter("bitcoin_features.tsv"));
    BufferedWriter rawwriter = new BufferedWriter(new FileWriter("bitcoin_raw_features.tsv"));
    BufferedWriter writer2 = new BufferedWriter(new FileWriter("bitcoin_ids.tsv"));

    // write header liner
    writer2.write("user\n");

    writeHeader(writer);
    writeHeader(rawwriter);

   // int max = 10;
    List<Features> features = new ArrayList<Features>();
    int skipped = 0;
    Map<Integer,Features> userToFeatures = new TreeMap<Integer,Features>();
    for (Integer user : users) {
     // logger.debug("user " + user);
      Features featuresForUser = getFeaturesForUser(transForUsers, user);
      if (featuresForUser != null) {
        features.add(featuresForUser);
        userToFeatures.put(user,featuresForUser);
      }
      else {
        skipped++;
      }
     // if (c++ > max) break;
    }

    logger.info("skipped " + skipped + " out of " + users.size() + " users");
    // normalize mean and variance

    Features firstFeature = features.get(0);
    int numFeatures = firstFeature.other.length;
    DescriptiveStatistics[] summaries = getSummaries(features, numFeatures);

    // TODO : finish adding spectral features -- need weights, need to add mean/std for spectral features
   // double[] weightVector = new double[useSpectralFeatures ? 160 : 10];
    double[] weightVector = new double[] {
        statWeight, statWeight, iarrWeight, iarrWeight, statWeight, statWeight, iarrWeight, iarrWeight, ppWeight, ppWeight };

/*
    if (useSpectralFeatures) {
      Arrays.fill(weightVector,specWeight);
    }
    System.arraycopy(weightVector2, 0, weightVector, (useSpectralFeatures ? 150 : 0), weightVector2.length);
*/

    double[] means = new double[numFeatures];
    double[] stds = new double[numFeatures];

    for (int i = 0; i < numFeatures; i++) {
      means[i] = summaries[i].getMean();
      stds[i] = summaries[i].getStandardDeviation();
      logger.debug("feature " + i + " " + means[i] + " std " + stds[i] + " num " + summaries[i].getN());
    }

    int j = 0;
    for (Map.Entry<Integer, Features> userFeatPair : userToFeatures.entrySet()) {
      double[] featureVector = userFeatPair.getValue().other;
      int id = userFeatPair.getKey();
      writer.write(id + "\t");
      rawwriter.write(id + "\t");
      writer2.write(id + "\n");

      if (useSpectralFeatures) {
         // TODO write out features, maybe to a separate file?
      }
      for (int i = 0; i < numFeatures; i++) {
        double v = featureVector[i];
        double finalValue = ((v - means[i]) / stds[i]) * weightVector[i];
        writer.write(finalValue + ((i == numFeatures - 1) ? "\n" : "\t"));
        rawwriter.write(v + ((i == numFeatures - 1) ? "\n" : "\t"));

        if (j++ % 10000 == 0) {
          writer.flush();
          rawwriter.flush();
        }
      }
    }

    writer.close();
    rawwriter.close();
    writer2.close();
  }

  private void writeHeader(BufferedWriter writer) throws IOException {
    writer.write("user\t");

    writer.write("credit_mean\tcredit_std\t");
    writer.write("credit_interarr_mean\tcredit_interarr_std\t");
    writer.write("debit_mean\tdebit_std\t");
    writer.write("debit_interarr_mean\tdebit_interarr_std\t");
    writer.write("perp_in\tperp_out\n");
  }

  private DescriptiveStatistics[] getSummaries(List<Features> features, int numFeatures) {
    DescriptiveStatistics [] summaries = new DescriptiveStatistics[numFeatures];

    for (int i = 0; i < numFeatures; i++) {
       summaries[i] = new DescriptiveStatistics();
    }
    for (Features featureVector : features) {
       for (int i = 0; i < numFeatures; i++ ) {
         summaries[i].addValue(featureVector.other[i]);
       }
    }
    return summaries;
  }

  /**
   * 160 features
   *
   *    *    name         = $nms
   credit_spec  = $credit_spec    x50
   debit_spec   = $debit_spec     x50
   merge_spec   = $merge_spec     x50
   credit_stats = $credit_stats
   credit_iarr  = $credit_iarr
   debit_stats  = $debit_stats
   debit_iarr   = $debit_iarr
   p_in         = $p_in           perp
   p_out        = $p_out          perp



   * @param transForUsers
   * @param user
   */
  private Features getFeaturesForUser(Map<Integer, UserFeatures> transForUsers, Integer user) {
    UserFeatures stats = transForUsers.get(user);

    if (stats == null) {
      logger.debug("no transactions for " + user + " in "  + transForUsers.keySet().size() + " keys.");
      return null;
    }

    double [] sfeatures = new double[150]; // later 160
    double [] features = new double[10]; // later 160
    if (stats.isValid()) {
      stats.calc();

      // TODO get this going later
      int i =0;
      if (useSpectral) {
        float[] creditSpec = stats.getCreditSpec();
        for (float aCreditSpec : creditSpec) sfeatures[i++] = aCreditSpec;
        float[] debitSpec = stats.getDebitSpec();
        for (float aDebitSpec : debitSpec) sfeatures[i++] = aDebitSpec;
        float[] mergeSpec = stats.getMergeSpec();
        for (float aMergeSpec : mergeSpec) sfeatures[i++] = aMergeSpec;
      }

      i = 0;

      List<Double> creditMeanAndStd = stats.getCreditMeanAndStd();
      List<Double> creditInterarrivalTimes = stats.getCreditInterarrMeanAndStd();
      List<Double> debitMeanAndStd = stats.getDebitMeanAndStd();
      List<Double> debitInterarrivalTimes = stats.getDebitInterarrMeanAndStd();

      double inPerplexity = stats.getInPerplexity();
      double outPerplexity = stats.getOutPerplexity();

      features[i++] = creditMeanAndStd.get(0);
      features[i++] = creditMeanAndStd.get(1);

      features[i++] = creditInterarrivalTimes.get(0);
      features[i++] = creditInterarrivalTimes.get(1);

      features[i++] = debitMeanAndStd.get(0);
      features[i++] = debitMeanAndStd.get(1);

      features[i++] = debitInterarrivalTimes.get(0);
      features[i++] = debitInterarrivalTimes.get(1);

      features[i++] = inPerplexity;
      features[i++] = outPerplexity;
    }
    else {
     // logger.debug("\tnot valid");
      features = null;
    }

    return new Features(sfeatures, features);
  }

  private static class Features {
    final double [] spectral;
    final double [] other;  // the non-spectral 10 features

    Features(double [] spectral, double [] other) { this.spectral = spectral; this.other = other; }
  }

/*  private List<DateAmount> getMove(DBConnection connection, Integer user, boolean credit) throws Exception {
    String sql = "select time, amount from TRANSACTIONS where " +
        (credit ? "target" : "source") +
        "="+user;

    PreparedStatement statement = connection.getConnection().prepareStatement(sql);
    ResultSet rs = statement.executeQuery();

    List<DateAmount> ids = new ArrayList<DateAmount>();
    int c = 0;

    while (rs.next()) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " +c);
      ids.add(new DateAmount(rs));
    }

    rs.close();
    statement.close();
    return  ids;
  }*/

/*
  private static class DateAmount {
    long time;
    double amount;

    public DateAmount(ResultSet rs) throws SQLException {
      time = rs.getLong(1); amount = rs.getDouble(2);
    }

    public String toString() { return "on " + new Timestamp(time).toString() + " : " +amount; }
  }
*/

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
  private void getStats(List<Transaction> transactions) {
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
  }

  /**
   * TODO : bitcoin too big to fit into memory... :(
   *
   *
   * @param connection
   * @return
   * @throws Exception
   * @deprecated
   */
  private List<Transaction> getTransactions(DBConnection connection) throws Exception {
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
  }

  private void writePairs(Collection<Integer> users,
                  String dataFilename, String outfile) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
    BufferedWriter writer = new BufferedWriter(new FileWriter(outfile));
    String line;
    int count = 0;
    long t0 = System.currentTimeMillis();
    int max = Integer.MAX_VALUE;
    int bad = 0;

  //  Map<Integer,UserFeatures> idToStats = new HashMap<Integer,UserFeatures>();
   //  Set<Long> connectedPairs = new HashSet<Long>(10000000);
                      int c= 0;
    Map<Integer,Set<Integer>> stot = new HashMap<Integer, Set<Integer>>();
    int skipped  = 0;
    while ((line = br.readLine()) != null) {
      count++;
      if (count > max) break;
      String[] split = line.split("\\s+"); // 4534248 25      25      2013-01-27 22:41:38     9.91897304
      if (split.length != 6) {
        bad++;
        if (bad <10) logger.warn("badly formed line " + line);
      }

      int source = Integer.parseInt(split[1]);
      int target = Integer.parseInt(split[2]);
      if (users.contains(source) && users.contains(target)) {
        Set<Integer> integers = stot.get(source);
        if (integers == null) stot.put(source, integers = new HashSet<Integer>());
        if (!integers.contains(target)) integers.add(target);
      }
      else {
        skipped++;
      }
     // long key = storeTwo(source, target);
      //if (!connectedPairs.contains(key)) connectedPairs.add(key);
      if (c++ % 1000000 == 0) logger.debug("read " + c);


      if (count % 10000000 == 0) {
        logger.debug("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count
            + " ms/read");
      }
    }
    logger.debug("skipped " + skipped + " entries");
    //for (Long pair : connectedPairs) writer.write(pair+"\n");
    int cc = 0;
    for (Map.Entry<Integer, Set<Integer>> pair : stot.entrySet()) {
       for (Integer i : pair.getValue()) {
         writer.write(storeTwo(pair.getKey(),i)+"\n");
         cc++;
       }
    }
    writer.close();
    logger.debug("wrote " + cc+ " pairs.");
    if (bad > 0) logger.warn("Got " + bad + " transactions...");
  }

  private long storeTwo(long low, long high) {
    long combined = low;
    combined += high << 32;
    return combined;
  }

  private Map<Integer,UserFeatures> getTransForUsers(String dataFilename, Collection<Integer> users) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
    String line;
    int count = 0;
    long t0 = System.currentTimeMillis();
    int max = Integer.MAX_VALUE;
    int bad = 0;

    Map<Integer,UserFeatures> idToStats = new HashMap<Integer,UserFeatures>();

    while ((line = br.readLine()) != null) {
      count++;
      if (count > max) break;
      String[] split = line.split("\\s+"); // 4534248 25      25      2013-01-27 22:41:38     9.91897304
      if (split.length != 6) {
        bad++;
        if (bad <10) logger.warn("badly formed line " + line);
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
      if (count % 10000000 == 0) {
        logger.debug("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count
            + " ms/read");
      }
    }
    if (bad > 0) logger.warn("Got " + bad + " transactions...");
    return idToStats;
  }

  private void addTransaction(Map<Integer, UserFeatures> idToStats, int source, int target, long time, double amount) {
    UserFeatures sourceStats = idToStats.get(source);
    if (sourceStats == null) idToStats.put(source,sourceStats = new UserFeatures(source));
    UserFeatures targetStats = idToStats.get(target);
    if (targetStats == null) idToStats.put(target,targetStats = new UserFeatures(target));

    Transaction trans = new Transaction(source, target, time, amount);

    sourceStats.addDebit(trans);
    targetStats.addCredit(trans);
  }

  private long getTime(String[] split, boolean onlyGetDailyData, Calendar calendar) {
    long time;
    String yyyyMMdd = split[3];
    if (onlyGetDailyData) {
      getDay(calendar, yyyyMMdd);
    }
    else {
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
   * The features we collect for each user (160)
   *
   * credit spectrum (50 values)
   * debit spectrum (50 values)
   * merged spectrum  (50 values)
   * credit mean, std
   * credit inter arrival times mean, std
   * debit mean, std
   * debit inter arrival times mean, std
   * incoming link perplexity
   * outgoing link perplexity
   *
   */
  private class UserFeatures {
    private final int id;
    final List<Transaction> debits = new ArrayList<Transaction>();
    final List<Transaction> credits= new ArrayList<Transaction>();
    final Map<Integer,Integer> targetToCount = new HashMap<Integer, Integer>();
    final Map<Integer,Integer> sourceToCount = new HashMap<Integer, Integer>();

    float[] expandedDebits, expandedCredits, expandedMerged;
    float[] dspectrum,cspectrum,mspectrum;
    public UserFeatures(int id) { this.id = id;}

    /**
     * @see BitcoinFeatures#getTransForUsers(String, java.util.Collection)
     * @param t
     */
    public void addDebit(Transaction t) {
      debits.add(t);
      Integer outgoing = targetToCount.get(t.target);
      targetToCount.put(t.target, outgoing == null ? 1 : outgoing +1);
    }
    public void addCredit(Transaction t) {
      credits.add(t);

      Integer incoming = sourceToCount.get(t.source);
      sourceToCount.put(t.source, incoming == null ? 1 : incoming + 1);
    }

    public void calc() {
      Collections.sort(debits);
      Collections.sort(credits);

      expandedDebits = getExpanded(debits, period, true);
      expandedCredits = getExpanded(credits, period, false);
      expandedMerged = getExpanded(credits, debits, period);

      if (useSpectral) {
        dspectrum = getSpectrum(expandedDebits);
        cspectrum = getSpectrum(expandedCredits);
        mspectrum = getSpectrum(expandedMerged);
      }
    }

    public List<Double> getCreditMeanAndStd() {
      if (credits.isEmpty()) {
        return EMPTY_DOUBLES;
      }
      DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
      for (Transaction transaction : credits) {  descriptiveStatistics.addValue(transaction.amount);  }
      return Arrays.asList(descriptiveStatistics.getMean(), descriptiveStatistics.getStandardDeviation());
    }

    public List<Double>  getDebitMeanAndStd() {
      if (debits.isEmpty()) {
        return EMPTY_DOUBLES;
      }
      DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
      for (Transaction transaction : debits) {  descriptiveStatistics.addValue(-transaction.amount);  }
      return Arrays.asList(descriptiveStatistics.getMean(), descriptiveStatistics.getStandardDeviation());
    }

    private float[] getExpanded(List<Transaction> debits, PERIOD period, boolean subtract) {
      long quanta = period.equals(PERIOD.DAY) ? DAY_IN_MILLIS : period.equals(PERIOD.HOUR) ? HOUR_IN_MILLIS : DAY_IN_MILLIS;

      if (debits.isEmpty()) return new float[0];

      long first = debits.get(0).time;
      long last = debits.get(debits.size() - 1).time;
      long bins = (last-first)/ quanta;
      float[] expDebits = new float[(int) bins+1];

      for (Transaction debit : debits) {
        long day = (debit.time-first)/ quanta;
        expDebits[(int)day] += (subtract ? -1 : +1) *debit.amount;
      }

      return expDebits;
    }

    private float[] getExpanded( List<Transaction> credits, List<Transaction> debits, PERIOD period) {
      long quanta = period.equals(PERIOD.DAY) ? DAY_IN_MILLIS : period.equals(PERIOD.HOUR) ? HOUR_IN_MILLIS : DAY_IN_MILLIS;

      // TODO : this is slow of course...
      List<Transaction> merged = new ArrayList<Transaction>(debits);
      merged.addAll(credits);
      Collections.sort(merged);

      long first = merged.get(0).time;
      long last = merged.get(merged.size() - 1).time;
      long bins = (last-first)/ quanta;
      float[] expDebits = new float[(int) bins+1];

      for (Transaction debit : debits) {
        long day = (debit.time-first)/ quanta;
        expDebits[(int)day] += -debit.amount;
      }

      for (Transaction credit : credits) {
        long day = (credit.time-first)/ quanta;
        expDebits[(int)day] += credit.amount;
      }

      return expDebits;
    }

    public double getInPerplexity() {
      return getPerplexity(sourceToCount);
    }

    public double getOutPerplexity() {
      return getPerplexity(targetToCount);
    }

    public double getPerplexity(Map<Integer, Integer> sourceToCount) {
      float total = 0f;
      for (int count : sourceToCount.values()) {
        total += (float) count;
      }
      float sum = 0;
      double log2 = Math.log(2);

     // logger.debug(this + " perp " + sourceToCount);
      for (int count : sourceToCount.values()) {
        float prob = (float) count / total;
     //   logger.debug("source " + count + " prob " + prob + " total " + total);
        sum += prob * (Math.log(prob) / log2);
      }
      return Math.pow(2, -sum);
    }

    public List<Double> getCreditInterarrMeanAndStd() {
      List<Long> creditInterarrivalTimes = getCreditInterarrivalTimes();
      if (creditInterarrivalTimes.isEmpty()) return EMPTY_DOUBLES;

      DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
      for (Long inter : creditInterarrivalTimes) {  descriptiveStatistics.addValue(inter);  }
      return Arrays.asList(descriptiveStatistics.getMean(), descriptiveStatistics.getStandardDeviation());
    }

    public List<Double> getDebitInterarrMeanAndStd() {
      List<Long> debitInterarrivalTimes = getDebitInterarrivalTimes();
      if (debitInterarrivalTimes.isEmpty()) return EMPTY_DOUBLES;

      DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
      for (Long inter : debitInterarrivalTimes) {  descriptiveStatistics.addValue(inter);  }
      return Arrays.asList(descriptiveStatistics.getMean(), descriptiveStatistics.getStandardDeviation());
    }

    private List<Long> getDebitInterarrivalTimes() {
      return getInterarrivalTimes(debits);
    }
    private List<Long> getCreditInterarrivalTimes() {
      return getInterarrivalTimes(credits);
    }

    public List<Long> getInterarrivalTimes(List<Transaction> times) {
      List<Long> diffs = new ArrayList<Long>();
      for (int i = 0; i < times.size() - 1; i += 1) {
        long diff = times.get(i + 1).time - times.get(i).time;
        if (period == PERIOD.DAY) {
          diff /= DAY_IN_MILLIS;
        } else if (period == PERIOD.HOUR) {
          diff /= HOUR_IN_MILLIS;
        }
        diffs.add(diff);
      }
      return diffs;
    }

    public boolean isValid() {
    //  logger.debug(this + " " + " num debits " + debits.size() + " num credits " + credits.size());
      return debits.size() > MIN_DEBITS || credits.size() > MIN_CREDITS;
    }

    public float[] getCreditSpec() {
      return cspectrum;
    }

    public float[] getDebitSpec() {
      return dspectrum;
    }

    public float[] getMergeSpec() {
      return mspectrum;
    }

    public String toString() { return ""+id; }
  }

  /**
   * TODO : fill this in -- note we want only the real components:
   *
   * Julia from Wade:
   *  function spectrum(timeLine)
   data = timeLine["total"].data
   r = zeros(int(speclen/2) + 1)
   n = 0.0
   for i = 1:int(speclen/2):length(data)
   ds = (i + speclen > length(data)) ? [ data[i:length(data)]; zeros(speclen - (length(data) - i)) ] : data[i:(i+speclen)]
   r += abs(rfft(ds))
   n += 1
   end
   fdata = r / n
   fstep = 0.5 / (length(fdata) - 1)

   tfreq = DataFrame()
   tfreq["power"]  = fdata[2:end]

   return tfreq
   end
   *
   */

  private float[] getSpectrum(float[] signal) {
    return getSpectrum(signal, SPECLEN);
  }

  /**
   * Skip first real component, since just the sum of the inputs
   *
   * TODO : we always have a zero at the end of the results -- seems like we always have n-1 fft values after
   *        throwing away DC component.
   *
   * @param signal
   * @param speclen
   * @return
   */
  private float[] getSpectrum(float[] signal,int speclen) {
    FloatFFT_1D fft = new FloatFFT_1D(speclen);
    int half = speclen / 2;
    float[] tmp = new float[speclen];
    float[] r = new float[half];
    float n = 0;

    for (int i = 0; i < signal.length; i += half) {
      int length = speclen;
      if (i+length > signal.length){
        Arrays.fill(tmp,0f);
        length = signal.length - i;
      //  logger.debug("at end " + i + " len " + length + " vs " + signal.length);
      }
      System.arraycopy(signal,i,tmp,0, length);
      //for (int x = 0; x < tmp.length; x++) logger.debug(x + " : " + tmp[x]);
      fft.realForward(tmp);
      // int k = 0;
      for (int j = 1; j < half; j++) {
        int i1 = j * 2;
     //   logger.debug("at " + i1);
        float realComponent = tmp[i1];
        r[j-1] = Math.abs(realComponent);
      }
      n += 1;
    }
    for (int i =0; i < r.length; i++) r[i] /= n;

    return r;
  }

  /**
   * NOTE : throws out "supernode" #25
   * @param connection
   * @return
   * @throws Exception
   */
  private Collection<Integer> getUsers(DBConnection connection) throws Exception {
    long then = System.currentTimeMillis();

    String sql = //"select distinct source from TRANSACTIONS";
                 "select source, count(*) as cnt from transactions " +
                     "where source <> " +
                     BITCOIN_OUTLIER +
                     " "+
                     "group by source having cnt > " +
                     MIN_TRANSACTIONS +
                     (LIMIT ? " limit " +
                         USER_LIMIT : "");
    PreparedStatement statement = connection.getConnection().prepareStatement(sql);
    ResultSet rs = statement.executeQuery();
    Set<Integer> ids = new HashSet<Integer>();
    int c = 0;

    while (rs.next()) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " +c);
      ids.add(rs.getInt(1));
    }
    long now = System.currentTimeMillis();
     logger.debug("took " +(now-then) + " to read " + ids.size() + " users");

    rs.close();
    statement.close();
    return  ids;
   }


  private static class Transaction implements Comparable<Transaction> {
    int source;
    int target;
    long time;
    double amount;
    public Transaction(ResultSet rs) throws Exception {
      int i = 1;
      source = rs.getInt(i++);
      target = rs.getInt(i++);
      //time = rs.getTimestamp(i++).getTime();
      time = rs.getLong(i++);
      amount = rs.getDouble(i);
    }

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

  public static void main(String [] args) {
    try {
      String dataFilename = null;
      boolean useSpectralFeatures = false;
      if (args.length > 0) {
        dataFilename = args[0];
        logger.debug("got " + dataFilename);
      }
      if (args.length > 1) {
        useSpectralFeatures = args[1].toLowerCase().contains("spectral");
      }
      long then = System.currentTimeMillis();

      BitcoinFeatures bitcoin = new BitcoinFeatures(new H2Connection("bitcoin", 38000000), dataFilename,useSpectralFeatures);

      long now = System.currentTimeMillis();
      logger.debug("took " +(now-then) + " millis to generate features");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
