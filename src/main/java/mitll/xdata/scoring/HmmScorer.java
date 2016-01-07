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

package mitll.xdata.scoring;

import be.ac.ulg.montefiore.run.jahmm.Hmm;
import be.ac.ulg.montefiore.run.jahmm.ObservationVector;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

public class HmmScorer {
    private static final Logger logger = Logger.getLogger(HmmScorer.class);

    /** Transactions from query (model) graph. */
    private final List<Transaction> transactions;

    /** Dummy transaction to mark transition to END state. */
    private final Transaction END_TRANSACTION;

    private final int numStates;

    private final Hmm<ObservationVector> hmm;

    /**
     * @param transactions
     * @param timeThresholdMillis
     *            max time allowed between phases
     * @param bandwidth
     *            bandwidth for kernel density estimators for observation distributions
     *            @see mitll.xdata.binding.Binding#rescoreWithHMM
     */
    public HmmScorer(List<Transaction> transactions, long timeThresholdMillis, double bandwidth) {
        this.transactions = transactions;
        List<List<Transaction>> observationsByState = partitionObservations(timeThresholdMillis);
        // create dummy observation to mark transition to END state
        END_TRANSACTION = createEndTransaction(transactions.get(0).getFeatures().length);
        // add one END state
        numStates = observationsByState.size() + 1;
        double[] pi = pi(observationsByState);
        double[][] A = A(observationsByState);
        List<OpdfKernelDensityEstimator> opdfs = createObservationPDFs(observationsByState, bandwidth);
        hmm = new Hmm<ObservationVector>(pi, A, opdfs);

        logger.debug("numStates = " + numStates);
        // logger.debug("pi = " + arrayToString(pi));
        // logger.debug("A = " + matrixToString(A));
    }

    /**
     * Sorts copy of List<Transaction>.
     */
    private List<Transaction> sort(List<Transaction> list) {
        List<Transaction> sorted = new ArrayList<Transaction>(list);
        Collections.sort(sorted, new Comparator<Transaction>() {
            @Override
            public int compare(Transaction transaction1, Transaction transaction2) {
                long t1 = transaction1.getTime();
                long t2 = transaction2.getTime();
                if (t1 < t2) {
                    return -1;
                } else if (t1 > t2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
        return sorted;
    }

    /**
     * @return Observation indices grouped by HMM state.
     * @see #HmmScorer(java.util.List, long, double)
     */
    private List<List<Transaction>> partitionObservations(long timeThresholdMillis) {
        List<List<Transaction>> observationsByState = new ArrayList<List<Transaction>>();
        List<Transaction> sorted = sort(transactions);

        Transaction current = sorted.get(0);
        Transaction last = null;
        List<Transaction> currentState = new ArrayList<Transaction>();
        currentState.add(current);
        observationsByState.add(currentState);
        for (int i = 1; i < sorted.size(); i++) {
            last = current;
            current = sorted.get(i);
            if (current.getTime() - last.getTime() <= timeThresholdMillis) {
                // add observation to current state
                currentState.add(current);
            } else {
                // create new state
                currentState = new ArrayList<Transaction>();
                currentState.add(current);
                observationsByState.add(currentState);
            }
        }

        return observationsByState;
    }

    /**
     * @return Initial probability distribution over HMM states.
     */
    private double[] pi(List<List<Transaction>> observationsByState) {
        double[] pi = new double[numStates];
        // put most weight on first state?
        // or maybe take number of observations per state into account?
        double firstStateProbability = 0.9;
        double otherProbability = (1.0 - firstStateProbability) / (numStates - 1.0);
        pi[0] = firstStateProbability;
        for (int i = 1; i < numStates - 1; i++) {
            pi[i] = otherProbability;
        }
        pi[numStates - 1] = 0.0;
        return pi;
    }

    /**
     * @return State transition probabilities.
     */
    private double[][] A(List<List<Transaction>> observationsByState) {
        double[][] A = new double[numStates][numStates];

        int totalObservations = transactions.size();

        // P(i --> i) = number of observations in state i / total observations
        // P(i --> j) = 1 - Aii - P(end) if j == i + 1 (i.e., is the next state)
        // P(i --> end) = P(end)

        double toEndState = 1e-15;

        for (int i = 0; i < numStates - 1; i++) {
            // initialize row to zero
            for (int j = 0; j < numStates; j++) {
                A[i][j] = 0.0;
            }
            if (i < numStates - 2) {
                // spread probability over i and i+1
                A[i][i] = observationsByState.get(i).size() / (1.0 * totalObservations);
                A[i][i + 1] = 1.0 - A[i][i] - toEndState;
                A[i][numStates - 1] = toEndState;
            } else if (i == numStates - 2) {
                // state right before END state
                A[i][i] = observationsByState.get(i).size() / (1.0 * totalObservations);
                A[i][i + 1] = 1.0 - A[i][i];
            }
        }

        // END state
        A[numStates - 1][numStates - 1] = 1.0;

        return A;
    }

    /**
     * Creates dummy observation to mark transition to END state.
     * 
     * It will have extremely low probability of being emitted from any state other than END, while having very high
     * probability (near 1) of being emitted in END state.
     */
    private Transaction createEndTransaction(int numFeatures) {
        // double[] features = new double[observationsByState.get(0).get(0).getFeatures().length];
        double[] features = new double[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            features[i] = 1e6;
        }
      return new Transaction("", "", 0, features);
    }

    /**
     * @return Observation probability distribution for each state in HMM.
     */
    private List<OpdfKernelDensityEstimator> createObservationPDFs(List<List<Transaction>> observationsByState,
            double bandwidth) {
        List<OpdfKernelDensityEstimator> opdfs = new ArrayList<OpdfKernelDensityEstimator>();
        for (List<Transaction> stateTransactions : observationsByState) {
            OpdfKernelDensityEstimator opdf = new OpdfKernelDensityEstimator(ovl(stateTransactions), bandwidth);
            opdfs.add(opdf);
        }

        // make very singular distribution for END state
        List<Transaction> transactions = new ArrayList<Transaction>();
        transactions.add(END_TRANSACTION);
        OpdfKernelDensityEstimator opdf = new OpdfKernelDensityEstimator(ovl(transactions), 1000.0);
        opdfs.add(opdf);
        
        return opdfs;
    }

    private List<Transaction> getTransactions() {
        return transactions;
    }

    /**
     * Converts Transaction to ObservationVector.
     */
    private ObservationVector ov(Transaction transaction) {
        return new ObservationVector(transaction.getFeatures());
    }

    /**
     * Converts List<Transaction> to List<ObservationVector>.
     */
    private List<ObservationVector> ovl(List<Transaction> transactions) {
        List<ObservationVector> observations = new ArrayList<ObservationVector>();
        for (Transaction transaction : transactions) {
            observations.add(ov(transaction));
        }
        return observations;
    }

    /**
     * Converts double array to ObservationVector.
     */
    private ObservationVector ov(double[] features) {
        return new ObservationVector(features);
    }

    /**
     * @return Log probability of HMM generating this sequence of transactions.
     */
    public double score(List<Transaction> transactions) {
        List<Transaction> sorted = sort(transactions);
        sorted.add(END_TRANSACTION);
        return hmm.lnProbability(ovl(sorted));
    }

    /**
     * Makes a test transaction with feature vector in R^2.
     */
    private static Transaction mt2(String date, double x1, double x2) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        long time = sdf.parse(date).getTime();
        double[] features = new double[] { x1, x2 };
        return new Transaction("source", "target", time, features);
    }

    private static String arrayToString(double[] x) {
        String s = "";
        for (int i = 0; i < x.length; i++) {
            if (i > 0)
                s += ", ";
            s += x[i];
        }
        return s;
    }

  private static String matrixToString(double[][] x) {
        String s = "";
        for (int i = 0; i < x.length; i++) {
            if (i > 0)
                s += ";   ";
            s += arrayToString(x[i]);
        }
        return s;
    }

    public static void main(String[] args) throws Exception {
        List<Transaction> transactions = new ArrayList<Transaction>();

        transactions.add(mt2("2013-07-25T00:00:00", 0.1, 0.1));

        transactions.add(mt2("2013-07-27T00:00:00", 3, 10));
        transactions.add(mt2("2013-07-27T02:00:00", 4, 9));
        transactions.add(mt2("2013-07-27T04:00:00", 20, 1));
        transactions.add(mt2("2013-07-27T06:00:00", 21, 2));

        transactions.add(mt2("2013-07-29T00:00:00", 1, 5));

        transactions.add(mt2("2013-08-01T00:00:00", 5, 5));
        transactions.add(mt2("2013-08-01T02:00:00", 6, 4));

        // hour
        long hour = 3600L * 1000L;
        long day = 24L * hour;
        long week = 7L * day;

        long timeThresholdMillis = 1L * day;
        double bandwidth = 0.5;
        HmmScorer scorer = new HmmScorer(transactions, timeThresholdMillis, bandwidth);

        // score original transactions we "trained" on
        logger.debug("scorer.score(transactions) = " + scorer.score(transactions));

        // score shorter list of transactions
        logger.debug("scorer.score(transactions.subList) = " + scorer.score(transactions.subList(2, 4)));

        // change feature vectors for some of the transactions
        transactions.get(0).setFeatures(new double[] { 3, -0.2 });
        transactions.get(1).setFeatures(new double[] { 2, 20 });
        logger.debug("scorer.score(transactions) = " + scorer.score(transactions));

        // shuffle the times of the transactions
        List<Transaction> shuffled = new ArrayList<Transaction>(transactions);
        Random rng = new Random();
        for (int i = 0; i < shuffled.size(); i++) {
            int j = rng.nextInt(shuffled.size());
            long temp = shuffled.get(i).getTime();
            shuffled.get(i).setTime(shuffled.get(j).getTime());
            shuffled.get(j).setTime(temp);
        }

        logger.debug("scorer.score(shuffled) = " + scorer.score(shuffled));

        logger.debug("done");
    }
}
