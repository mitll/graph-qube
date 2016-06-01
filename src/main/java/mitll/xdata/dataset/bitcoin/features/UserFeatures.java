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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;

import java.util.*;

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
public class UserFeatures {
  private static final Logger logger = Logger.getLogger(UserFeatures.class);
  private static final int MIN_DEBITS = 5;
  private static final int MIN_CREDITS = 5;
  private static final List<Double> EMPTY_DOUBLES = Arrays.asList(0d, 0d);

  private static final int HOUR_IN_MILLIS = 60 * 60 * 1000;
  private static final long DAY_IN_MILLIS = 24 * HOUR_IN_MILLIS;

  private enum PERIOD {HOUR, DAY, WEEK, MONTH}

  private final PERIOD period = PERIOD.DAY; // bin by day for now

  private final long id;
  private final List<Transaction> debits = new ArrayList<>();
  private final List<Transaction> credits = new ArrayList<>();
  private final Map<Long, Integer> targetToCount = new HashMap<>();
  private final Map<Long, Integer> sourceToCount = new HashMap<>();
  private String type;

  private final boolean useSpectral = false;
  private float[] expandedDebits;
  private float[] expandedCredits;
  private float[] expandedMerged;
  private float[] dspectrum;
  private float[] cspectrum;
  private float[] mspectrum;

  /**
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestUnchartedTransactions#addTransaction(Map, long, long, long, double)
   * @param id
   */
  public UserFeatures(long id) {
    this.id = id;
  }

  /**
   * @param t
   * @see BitcoinFeatures#getTransForUsers(String, java.util.Collection)
   */
  public void addDebit(Transaction t) {
    debits.add(t);
    Integer outgoing = targetToCount.get(t.getTarget());
    targetToCount.put(t.getTarget(), outgoing == null ? 1 : outgoing + 1);
  }

  public void addCredit(Transaction t) {
    credits.add(t);

    Integer incoming = sourceToCount.get(t.getSource());
    sourceToCount.put(t.getSource(), incoming == null ? 1 : incoming + 1);
  }

  void calc() {
    Collections.sort(debits);
    Collections.sort(credits);

    expandedDebits = getExpanded(debits, period, true);
    expandedCredits = getExpanded(credits, period, false);
    expandedMerged = getExpanded(credits, debits, period);

//      if (useSpectral) {
//        dspectrum = getSpectrum(expandedDebits);
//        cspectrum = getSpectrum(expandedCredits);
//        mspectrum = getSpectrum(expandedMerged);
//      }
  }

   List<Double> getCreditMeanAndStd() {
    if (credits.isEmpty()) {
      return EMPTY_DOUBLES;
    }
    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
    for (Transaction transaction : credits) {
      descriptiveStatistics.addValue(transaction.getAmount());
    }
    return Arrays.asList(descriptiveStatistics.getMean(), descriptiveStatistics.getStandardDeviation());
  }

  public List<Double> getDebitMeanAndStd() {
    if (debits.isEmpty()) {
      return EMPTY_DOUBLES;
    }
    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
    for (Transaction transaction : debits) {
      descriptiveStatistics.addValue(-transaction.getAmount());
    }
    return Arrays.asList(descriptiveStatistics.getMean(), descriptiveStatistics.getStandardDeviation());
  }

  private float[] getExpanded(List<Transaction> debits, PERIOD period, boolean subtract) {
    long quanta = period.equals(PERIOD.DAY) ? DAY_IN_MILLIS : period.equals(PERIOD.HOUR) ? HOUR_IN_MILLIS : DAY_IN_MILLIS;

    if (debits.isEmpty()) return new float[0];

    long first = debits.get(0).getTime();
    long last = debits.get(debits.size() - 1).getTime();
    long bins = (last - first) / quanta;
    float[] expDebits = new float[(int) bins + 1];

    for (Transaction debit : debits) {
      long day = (debit.getTime() - first) / quanta;
      expDebits[(int) day] += (subtract ? -1 : +1) * debit.getAmount();
    }

    return expDebits;
  }

  private float[] getExpanded(Collection<Transaction> credits, Collection<Transaction> debits, PERIOD period) {
    long quanta = period.equals(PERIOD.DAY) ? DAY_IN_MILLIS : period.equals(PERIOD.HOUR) ? HOUR_IN_MILLIS : DAY_IN_MILLIS;

    // TODO : this is slow of course...
    List<Transaction> merged = new ArrayList<>(debits);
    merged.addAll(credits);
    Collections.sort(merged);

    long first = merged.get(0).getTime();
    long last = merged.get(merged.size() - 1).getTime();
    long bins = (last - first) / quanta;
    float[] expDebits = new float[(int) bins + 1];

    for (Transaction debit : debits) {
      long day = (debit.getTime() - first) / quanta;
      expDebits[(int) day] += -debit.getAmount();
    }

    for (Transaction credit : credits) {
      long day = (credit.getTime() - first) / quanta;
      expDebits[(int) day] += credit.getAmount();
    }

    return expDebits;
  }

  double getInPerplexity() {
    return getPerplexity(sourceToCount);
  }

  double getOutPerplexity() {
    return getPerplexity(targetToCount);
  }

  private double getPerplexity(Map<Long, Integer> sourceToCount) {
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

  List<Double> getCreditInterarrMeanAndStd() {
    Collection<Long> creditInterarrivalTimes = getCreditInterarrivalTimes();
    if (creditInterarrivalTimes.isEmpty()) return EMPTY_DOUBLES;

    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
    for (Long inter : creditInterarrivalTimes) {
      descriptiveStatistics.addValue(inter);
    }
    double mean = descriptiveStatistics.getMean();
    if (Double.isNaN(mean)) {
      logger.error("got NAN " + creditInterarrivalTimes);
    }
    return Arrays.asList(mean, descriptiveStatistics.getStandardDeviation());
  }

  public List<Double> getDebitInterarrMeanAndStd() {
    Collection<Long> debitInterarrivalTimes = getDebitInterarrivalTimes();
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

  private List<Long> getInterarrivalTimes(List<Transaction> times) {
    List<Long> diffs = new ArrayList<>();
    for (int i = 0; i < times.size() - 1; i += 1) {
      long diff = times.get(i + 1).getTime() - times.get(i).getTime();
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
