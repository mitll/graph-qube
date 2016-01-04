package mitll.xdata.dataset.bitcoin.features;

import edu.emory.mathcs.jtransforms.fft.FloatFFT_1D;
import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.scoring.FeatureNormalizer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
public class BitcoinFeaturesBase {
  private static final int MIN_TRANSACTIONS = 10;
  private static final boolean LIMIT = false;
  private static final int BITCOIN_OUTLIER = 25;
  private static final int USER_LIMIT = 10000000;
  private static final int MIN_DEBITS = 5;
  private static final int MIN_CREDITS = 5;
  private static final List<Double> EMPTY_DOUBLES = Arrays.asList(0d, 0d);
  private static final int SPECLEN = 100;//
  private static final int NUM_STANDARD_FEATURES = 10;
  //public static final String BITCOIN_IDS_TSV = "bitcoin_ids.tsv";
  private static final String BITCOIN_RAW_FEATURES_TSV = "bitcoin_raw_features.tsv";
  public static final String BITCOIN_FEATURES_STANDARDIZED_TSV = "bitcoin_features_standardized.tsv";
  // private static final boolean USE_SPECTRAL_FEATURES = true;
  // double specWeight = 1.0;
  // private final double statWeight = 15.0;
  // private final double iarrWeight = 30.0;
  // private final double ppWeight   = 20.0;
  private boolean useSpectral = false;

  private static final int HOUR_IN_MILLIS = 60 * 60 * 1000;
  private static final long DAY_IN_MILLIS = 24 * HOUR_IN_MILLIS;

  private enum PERIOD {HOUR, DAY, WEEK, MONTH}

  private final PERIOD period = PERIOD.DAY; // bin by day for now
  private static final Logger logger = Logger.getLogger(BitcoinFeatures.class);


/*  public BitcoinFeaturesBase(String h2DatabaseFile, String writeDirectory, String datafile) throws Exception {
    this(new H2Connection(h2DatabaseFile, 38000000), writeDirectory, datafile, false);
  }*/

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
/*  private BitcoinFeaturesBase(DBConnection connection, String writeDirectory, String datafile, boolean useSpectralFeatures) throws Exception {
    long then = System.currentTimeMillis();
    this.useSpectral = useSpectralFeatures;
    // long now = System.currentTimeMillis();
    // logger.debug("took " +(now-then) + " to read " + transactions);
    logger.debug("reading users from db " + connection);

    Collection<Integer> users = getUsers(connection);

    String pairsFilename = writeDirectory + "pairs.txt";

    writePairs(users, datafile, pairsFilename);

    Map<Integer, UserFeatures> transForUsers = getTransForUsers(datafile, users);

    writeFeatures(connection, writeDirectory, then, users, transForUsers);
  }*/

  protected void writeFeatures(DBConnection connection, String writeDirectory, long then, Collection<Integer> users, Map<Integer, UserFeatures> transForUsers) throws Exception {
    long now = System.currentTimeMillis();
    logger.debug("took " + (now - then) + " to read " + transForUsers.size() + " user features");

    //BufferedWriter writer = new BufferedWriter(new FileWriter("bitcoin_features.tsv"));
    BufferedWriter rawWriter = new BufferedWriter(new FileWriter(new File(writeDirectory, BITCOIN_RAW_FEATURES_TSV)));
    //BufferedWriter idsWriter = new BufferedWriter(new FileWriter(BITCOIN_IDS_TSV));
    BufferedWriter standardFeatureWriter = new BufferedWriter(new FileWriter(new File(writeDirectory, BITCOIN_FEATURES_STANDARDIZED_TSV)));

    // write header liner
    //  idsWriter.write("user\n");

    //writeHeader(writer);
    writeHeader(rawWriter);
    writeHeader(standardFeatureWriter);

    List<Features> features = new ArrayList<Features>();
    Map<Integer, Features> userToFeatures = new TreeMap<Integer, Features>();

    // TODO - change size if we add spectral features
    Map<Integer, Integer> userToIndex = new HashMap<Integer, Integer>();

    populateUserToFeatures(users, transForUsers, features, userToFeatures, userToIndex);

    // copy features into a matrix

    double[][] standardizedFeatures = getStandardizedFeatures(features);
    // getStandardizationStats();

    writeFeaturesToFiles(rawWriter, standardFeatureWriter, userToFeatures, userToIndex, standardizedFeatures);

    writeFeaturesToDatabase(connection, userToFeatures, userToIndex, standardizedFeatures);

    //writer.close();
    rawWriter.close();
    // idsWriter.close();
    standardFeatureWriter.close();

    connection.closeConnection();
  }

  /**
   * Calculate features for each user from the raw features in UserFeatures
   *
   * @param users
   * @param transForUsers
   * @param features
   * @param userToFeatures
   * @param userToIndex
   */
  private void populateUserToFeatures(Collection<Integer> users,
                                      Map<Integer, UserFeatures> transForUsers,
                                      List<Features> features,
                                      Map<Integer, Features> userToFeatures,
                                      Map<Integer, Integer> userToIndex) {
    int skipped = 0;
    int count = 0;
    for (Integer user : users) {
      // logger.debug("user " + user);
      Features featuresForUser = getFeaturesForUser(transForUsers, user);
      if (featuresForUser != null) {
        features.add(featuresForUser);
 /*       if (count < 10) {
          logger.debug("mapping " + user + " to " + count + " features " + featuresForUser);
        }*/
        userToIndex.put(user, count++);
        userToFeatures.put(user, featuresForUser);
      } else {
        skipped++;
      }
    }
    logger.info("skipped " + skipped + " out of " + users.size() + " users who had less than 5 credits and less than 5 debits");
  }

  private void getStandardizationStats() {
    // normalize mean and variance

    //Features firstFeature = features.get(0);
    //int numFeatures = firstFeature.other.length;
    //DescriptiveStatistics[] summaries = getSummaries(features, numFeatures);

    // TODO : finish adding spectral features -- need weights, need to add mean/std for spectral features
    // double[] weightVector = new double[useSpectralFeatures ? 160 : 10];
/*
    double[] weightVector = new double[] {
        statWeight, statWeight, iarrWeight, iarrWeight, statWeight, statWeight, iarrWeight, iarrWeight, ppWeight, ppWeight };
*/

/*
    if (useSpectralFeatures) {
      Arrays.fill(weightVector,specWeight);
    }
    System.arraycopy(weightVector2, 0, weightVector, (useSpectralFeatures ? 150 : 0), weightVector2.length);
*/

/*    double[] means = new double[numFeatures];
    double[] stds = new double[numFeatures];

    // calculate means and standard deviations
    for (int i = 0; i < numFeatures; i++) {
      means[i] = summaries[i].getMean();
      stds[i] = summaries[i].getStandardDeviation();
      logger.debug("feature " + i + " " + means[i] + " std " + stds[i] + " num " + summaries[i].getN());
    }*/
  }

  private void writeFeaturesToFiles(BufferedWriter rawWriter, BufferedWriter standardFeatureWriter,
                                    Map<Integer, Features> userToFeatures, Map<Integer, Integer> userToIndex,
                                    double[][] standardizedFeatures) throws IOException {
    Features next = userToFeatures.values().iterator().next();
    int numFeatures = next.other.length;

    int j = 0;
    for (Map.Entry<Integer, Features> userFeatPair : userToFeatures.entrySet()) {
      Features value = userFeatPair.getValue();
      double[] featureVector = value.other;
      int id = userFeatPair.getKey();
      // writer.write(id + "\t");
      rawWriter.write(id + "\t");
      //idsWriter.write(id + "\n");
      standardFeatureWriter.write(id + "\t");

      //if (useSpectralFeatures) {
      // TODO write out features, maybe to a separate file?
      // }
      Integer userIndex = userToIndex.get(id);

      double[] standardizedFeature = standardizedFeatures[userIndex];

		  /*      if (id < 10) {
        logger.debug("user " + id + " index " + userIndex + " features " + value + " vs " + getDoubles(standardizedFeature));
      }*/
      for (int i = 0; i < numFeatures; i++) {
        double v = featureVector[i];
        //      double finalValue = ((v - means[i]) / stds[i]) * weightVector[i];
        String separator = (i == numFeatures - 1) ? "\n" : "\t";
        //    writer.write(finalValue + separator);
        rawWriter.write(v + separator);

        double standardizedValue = standardizedFeature[i];
        standardFeatureWriter.write(standardizedValue + separator);

        if (j++ % 10000 == 0) {
          //    writer.flush();
          rawWriter.flush();
          standardFeatureWriter.flush();
        }
      }
    }
  }

  private void writeFeaturesToDatabase(DBConnection dbConnection, Map<Integer, Features> userToFeatures, Map<Integer, Integer> userToIndex,
                                       double[][] standardizedFeatures) throws Exception {

    logger.info("writeFeaturesToDatabase");

    Connection connection = dbConnection.getConnection();
    new FeaturesSql().createUsersTable(connection);
    PreparedStatement statement;


    String[] columnLabels = {"USER", "CREDIT_MEAN", "CREDIT_STD",
        "CREDIT_INTERARR_MEAN", "CREDIT_INTERARR_STD",
        "DEBIT_MEAN", "DEBIT_STD",
        "DEBIT_INTERARR_MEAN", "DEBIT_INTERARR_STD",
        "PERP_IN", "PERP_OUT"};
    String columnLabelText = "(" + StringUtils.join(columnLabels, ", ") + ")";


    int numFeatures = columnLabels.length - 1;


    for (Map.Entry<Integer, Features> userFeatPair : userToFeatures.entrySet()) {

      int id = userFeatPair.getKey();
      Integer userIndex = userToIndex.get(id);
      double[] standardizedFeature = standardizedFeatures[userIndex];

      String featValueText = "(" + id + ", ";
      for (int i = 0; i < numFeatures; i++) {

        if (i != numFeatures - 1) {
          featValueText += Double.toString(standardizedFeature[i]) + ", ";
        } else {
          featValueText += Double.toString(standardizedFeature[i]) + ")";
        }
      }

      String sqlInsertVector = "insert into users " + columnLabelText + " values " + featValueText + ";";
      statement = connection.prepareStatement(sqlInsertVector);
      statement.executeUpdate();
      statement.close();
    }

    logger.info("writeFeaturesToDatabase - alter users table");

    // Insert default type into table (to possibly be overwritten by clustering output)
    String sqlInsertType = "alter table USERS add TYPE int not null default(1);";
    statement = connection.prepareStatement(sqlInsertType);
    statement.executeUpdate();
    statement.close();

    connection.close();
  }

  private static String getDoubles(double[] arr) {
    String val = "";
    for (double d : arr) val += d + " ";
    return val;
  }

  private double[][] getStandardizedFeatures(List<Features> features) {
    double[][] featureMatrix = new double[features.size()][NUM_STANDARD_FEATURES];
    int i = 0;
    for (Features feature : features) {
      double[] dest = featureMatrix[i++];
      if (feature.other == null) logger.error("huh? feature vector is null");
      System.arraycopy(feature.other, 0, dest, 0, NUM_STANDARD_FEATURES);
    }

    for (int j = 0; j < NUM_STANDARD_FEATURES; j++) { //num_feats = 10 for our raw bitcoin features
      if (j == 4) {//: #change polarity for mean debits
        for (int k = 0; k < features.size(); k++) {
          featureMatrix[k][j] = Math.log(1 + Math.abs(featureMatrix[k][j]));
        }
      } else if ((j == 8) | (j == 9)) { //#perplexity metric has min val = 1
        for (int k = 0; k < features.size(); k++) {
          featureMatrix[k][j] = Math.log(featureMatrix[k][j]);
        }
      } else {
        for (int k = 0; k < features.size(); k++) {
          featureMatrix[k][j] = Math.log(1 + Math.abs(featureMatrix[k][j]));
        }
      }
    }

    double lowerPercentile = 0.025;
    double upperPercentile = 0.975;
    FeatureNormalizer normalizer = new FeatureNormalizer(featureMatrix, lowerPercentile, upperPercentile);
    return normalizer.normalizeFeatures(featureMatrix);
  }

  private void writeHeader(BufferedWriter writer) throws IOException {
    writer.write("user\t");

    writer.write("credit_mean\tcredit_std\t");
    writer.write("credit_interarr_mean\tcredit_interarr_std\t");
    writer.write("debit_mean\tdebit_std\t");
    writer.write("debit_interarr_mean\tdebit_interarr_std\t");
    writer.write("perp_in\tperp_out\n");
  }

/*
  private DescriptiveStatistics[] getSummaries(List<Features> features, int numFeatures) {
    DescriptiveStatistics[] summaries = new DescriptiveStatistics[numFeatures];

    for (int i = 0; i < numFeatures; i++) {
      summaries[i] = new DescriptiveStatistics();
    }
    for (Features featureVector : features) {
      for (int i = 0; i < numFeatures; i++) {
        summaries[i].addValue(featureVector.other[i]);
      }
    }
    return summaries;
  }
*/

  /**
   * 160 features
   * <p>
   * *    name         = $nms
   * credit_spec  = $credit_spec    x50
   * debit_spec   = $debit_spec     x50
   * merge_spec   = $merge_spec     x50
   * credit_stats = $credit_stats
   * credit_iarr  = $credit_iarr
   * debit_stats  = $debit_stats
   * debit_iarr   = $debit_iarr
   * p_in         = $p_in           perp
   * p_out        = $p_out          perp
   *
   * @param transForUsers
   * @param user
   * @see #BitcoinFeatures(mitll.xdata.db.DBConnection, String, String, boolean)
   */
  private Features getFeaturesForUser(Map<Integer, UserFeatures> transForUsers, Integer user) {
    UserFeatures stats = transForUsers.get(user);

    if (stats == null) {
      //logger.debug("no transactions for " + user + " in "  + transForUsers.keySet().size() + " keys.");
      return null;
    }

    double[] sfeatures = new double[150]; // later 160
    double[] features = new double[NUM_STANDARD_FEATURES]; // later 160
    if (stats.isValid()) {
      stats.calc();

      // TODO get this going later
      int i = 0;
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
    } else {
      //  logger.debug("\tuser " +user+ " is not valid");
      //features = null;
      return null;
    }

    return new Features(sfeatures, features);
  }

  private static class Features {
    final double[] spectral;
    final double[] other;  // the non-spectral 10 features

    Features(double[] spectral, double[] other) {
      this.spectral = spectral;
      this.other = other;
    }

    public String toString() {

      return "Features " + getDoubles(other);
    }
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
   * @deprecatedx
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
/*  private void writePairs(Collection<Integer> users,
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
    Map<Integer, Set<Integer>> stot = new HashMap<Integer, Set<Integer>>();
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

      skipped = getSkipped(users, stot, skipped, source, target);
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
  }*/

  protected int getSkipped(Collection<Integer> users, Map<Integer, Set<Integer>> stot, int skipped, int source, int target) {
    if (users.contains(source) && users.contains(target)) {
      Set<Integer> integers = stot.get(source);
      if (integers == null) stot.put(source, integers = new HashSet<Integer>());
      if (!integers.contains(target)) integers.add(target);
    } else {
      skipped++;
    }
    return skipped;
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
    logger.debug("wrote " + cc + " pairs.");
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
  private long storeTwo(long low, long high) {
    long combined = low;
    combined += high << 32;
    return combined;
  }

  /**
   * @param dataFilename
   * @param users        transactions must be between the subset of non-trivial users (who have more than 10 transactions)
   * @return
   * @throws Exception
   * @see #BitcoinFeatures(DBConnection, String, String, boolean)
   */
/*  protected Map<Integer, UserFeatures> getTransForUsers(String dataFilename, Collection<Integer> users) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
    String line;
    int count = 0;
    long t0 = System.currentTimeMillis();
    int max = Integer.MAX_VALUE;
    int bad = 0;

    Map<Integer, UserFeatures> idToStats = new HashMap<Integer, UserFeatures>();

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
  }*/

  void addTransaction(Map<Integer, UserFeatures> idToStats, int source, int target, long time, double amount) {
    UserFeatures sourceStats = idToStats.get(source);
    if (sourceStats == null) idToStats.put(source, sourceStats = new UserFeatures(source));
    UserFeatures targetStats = idToStats.get(target);
    if (targetStats == null) idToStats.put(target, targetStats = new UserFeatures(target));

    Transaction trans = new Transaction(source, target, time, amount);

    sourceStats.addDebit(trans);
    targetStats.addCredit(trans);
  }
/*
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

  *//*  String hour = hhmmss.substring(0, 2);
    String min = hhmmss.substring(3, 5);
    String sec = hhmmss.substring(6, 8);*//*

    // logger.debug("value " +year + " " + month + " " + day + " " + hour + " " + min + " " + sec);

    calendar.set(Integer.parseInt(year), imonth, Integer.parseInt(day));
  }*/

  /**
   * The features we collect for each user (160)
   * <p>
   * credit spectrum (50 values)
   * debit spectrum (50 values)
   * merged spectrum  (50 values)
   * credit mean, std
   * credit inter arrival times mean, std
   * debit mean, std
   * debit inter arrival times mean, std
   * incoming link perplexity
   * outgoing link perplexity
   */
  class UserFeatures {
    private final int id;
    final List<Transaction> debits = new ArrayList<Transaction>();
    final List<Transaction> credits = new ArrayList<Transaction>();
    final Map<Integer, Integer> targetToCount = new HashMap<Integer, Integer>();
    final Map<Integer, Integer> sourceToCount = new HashMap<Integer, Integer>();

    float[] expandedDebits, expandedCredits, expandedMerged;
    float[] dspectrum, cspectrum, mspectrum;

    public UserFeatures(int id) {
      this.id = id;
    }

    /**
     * @param t
     * @see BitcoinFeatures#getTransForUsers(String, java.util.Collection)
     */
    public void addDebit(Transaction t) {
      debits.add(t);
      Integer outgoing = targetToCount.get(t.target);
      targetToCount.put(t.target, outgoing == null ? 1 : outgoing + 1);
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
      for (Transaction transaction : credits) {
        descriptiveStatistics.addValue(transaction.amount);
      }
      return Arrays.asList(descriptiveStatistics.getMean(), descriptiveStatistics.getStandardDeviation());
    }

    public List<Double> getDebitMeanAndStd() {
      if (debits.isEmpty()) {
        return EMPTY_DOUBLES;
      }
      DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
      for (Transaction transaction : debits) {
        descriptiveStatistics.addValue(-transaction.amount);
      }
      return Arrays.asList(descriptiveStatistics.getMean(), descriptiveStatistics.getStandardDeviation());
    }

    private float[] getExpanded(List<Transaction> debits, PERIOD period, boolean subtract) {
      long quanta = period.equals(PERIOD.DAY) ? DAY_IN_MILLIS : period.equals(PERIOD.HOUR) ? HOUR_IN_MILLIS : DAY_IN_MILLIS;

      if (debits.isEmpty()) return new float[0];

      long first = debits.get(0).time;
      long last = debits.get(debits.size() - 1).time;
      long bins = (last - first) / quanta;
      float[] expDebits = new float[(int) bins + 1];

      for (Transaction debit : debits) {
        long day = (debit.time - first) / quanta;
        expDebits[(int) day] += (subtract ? -1 : +1) * debit.amount;
      }

      return expDebits;
    }

    private float[] getExpanded(List<Transaction> credits, List<Transaction> debits, PERIOD period) {
      long quanta = period.equals(PERIOD.DAY) ? DAY_IN_MILLIS : period.equals(PERIOD.HOUR) ? HOUR_IN_MILLIS : DAY_IN_MILLIS;

      // TODO : this is slow of course...
      List<Transaction> merged = new ArrayList<Transaction>(debits);
      merged.addAll(credits);
      Collections.sort(merged);

      long first = merged.get(0).time;
      long last = merged.get(merged.size() - 1).time;
      long bins = (last - first) / quanta;
      float[] expDebits = new float[(int) bins + 1];

      for (Transaction debit : debits) {
        long day = (debit.time - first) / quanta;
        expDebits[(int) day] += -debit.amount;
      }

      for (Transaction credit : credits) {
        long day = (credit.time - first) / quanta;
        expDebits[(int) day] += credit.amount;
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
      for (Long inter : creditInterarrivalTimes) {
        descriptiveStatistics.addValue(inter);
      }
      return Arrays.asList(descriptiveStatistics.getMean(), descriptiveStatistics.getStandardDeviation());
    }

    public List<Double> getDebitInterarrMeanAndStd() {
      List<Long> debitInterarrivalTimes = getDebitInterarrivalTimes();
      if (debitInterarrivalTimes.isEmpty()) return EMPTY_DOUBLES;

      DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
      for (Long inter : debitInterarrivalTimes) {
        descriptiveStatistics.addValue(inter);
      }
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

    public String toString() {
      return "" + id;
    }
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

  /**
   * Filter out accounts that have less than {@link #MIN_TRANSACTIONS} transactions.
   * NOTE : throws out "supernode" #25
   *
   * @param connection
   * @return
   * @throws Exception
   */
  Collection<Integer> getUsers(DBConnection connection) throws Exception {
    long then = System.currentTimeMillis();

	  /*
     * Grab only those users having more than MIN_TRANSACTIONS total transactions
	   * (as either source or target); filter out BITCOIN_OUTLIER
	   */
	  /*
    String sql =
        "select source, count(*) as cnt from "+BitcoinBinding.TRANSACTIONS+" "+
            "where source <> " +
            BITCOIN_OUTLIER +
            " " +
            //"group by source having cnt > " +
            //MIN_TRANSACTIONS +
            (LIMIT ? " limit " + USER_LIMIT : "");
	   */

	  /*
	   * Execute updates to figure out
	   */
    String sql = "drop table temp if exists;" +
        " drop table temp2 if exists;" +
        " create table temp as select source as uid, count(*) as num_trans " +
        " from " + BitcoinBinding.TRANSACTIONS + " where source <> " + BITCOIN_OUTLIER + " group by source;" +
        " insert into temp (uid,num_trans)" +
        " select target, count(*) from " + BitcoinBinding.TRANSACTIONS +
        " where target <> " + BITCOIN_OUTLIER + " group by target;" +
        " drop table temp2 if exists;" +
        " create table temp2 as select uid, sum(num_trans) as tot_num_trans" +
        " from temp group by uid having tot_num_trans >= " + MIN_TRANSACTIONS + ";" +
        " drop table temp;" +
        " select * from temp2;";

    PreparedStatement statement = connection.getConnection().prepareStatement(sql);
    statement.executeUpdate();

	  /*
	   * Execute query to load in active-enough users...
	   */
    String sqlQuery = "select * from temp2;";
    statement = connection.getConnection().prepareStatement(sqlQuery);
    ResultSet rs = statement.executeQuery();

    Set<Integer> ids = new HashSet<Integer>();
    int c = 0;

    while (rs.next()) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " + c);
      ids.add(rs.getInt(1));
    }
    long now = System.currentTimeMillis();
    logger.debug("took " + (now - then) + " millis to read " + ids.size() + " users");

    rs.close();
    statement.close();
    return ids;
  }

  @SuppressWarnings("CanBeFinal")
  static class Transaction implements Comparable<Transaction> {
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

  private static final int MB = (1024 * 1024);

  void logMemory() {
    Runtime rt = Runtime.getRuntime();
    long free = rt.freeMemory();
    long used = rt.totalMemory() - free;
    long max = rt.maxMemory();
    logger.debug("heap info free " + free / MB + "M used " + used / MB + "M max " + max / MB + "M");
  }

/*  public static void main(String[] args) {
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
  }*/
}
