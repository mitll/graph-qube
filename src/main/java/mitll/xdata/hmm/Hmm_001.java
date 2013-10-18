package mitll.xdata.hmm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

public class Hmm_001<T extends Observation> {
	private static Logger logger = Logger.getLogger(Hmm_001.class);

	/** number of states (not counting special start and end states q_0 and q_F). */
	private int numStates;

	/** index of start state. */
	private int q_0;

	/** index of final (end) state. */
	private int q_F;

	/** transition probability matrix: (numStates + 2) x (numStates + 2). */
	private double[][] A;

	/**
	 * list of observation likelihoods (emission probabilities) for each state: b_i(o) = P(observation o emitted in
	 * state i). Note: There are no emissions in q_0 or q_F.
	 */
	private List<ObservationLikelihood<T>> b;

	public Hmm_001(double[][] A, List<ObservationLikelihood<T>> b) {
		this.A = A;
		this.b = b;
		this.numStates = b.size();

		// e.g., if 4 states, would have states: 0 (q_0), 1, 2, 3, 4, 5 (q_F)
		q_0 = 0;
		q_F = numStates + 1;
	}

	public double probability(List<Integer> states, List<T> observations) {
		return 0.0;
	}
	
	/**
	 * Computes probability of observation sequence.
	 */
	public double likelihood(List<T> observations) {
		return 0.0;
	}

	/**
	 * Finds optimal sequence of hidden states.
	 * 
	 * Follows pseudo-code from Jurafsky and Martin 2009, p. 186.
	 */
	public StateSequence decode(List<T> observations) {
		// Note: pseduo-code goes from t = 1, ..., T, while this code goes from t = 0, ..., (numObs - 1).
		// Note: emission probability likelihoods are 0-indexed in b list, so need to subtract 1 from state index

		int numObs = observations.size();

		// viterbi(s, t) = probability of of most probable path ending in state s at time t given observations o_1, ...,
		// o_t
		double[][] viterbi = new double[numStates + 2][numObs];

		// backpointer(s, t) = state at time t-1 for most probable path ending in state s at time t given observations
		int[][] backpointer = new int[numStates + 2][numObs];

		// initialize (computer probability of going from q_0 to each state and observing o_1)
		for (int s = 1; s <= numStates; s++) {
			viterbi[s][0] = A[q_0][s] * b.get(s - 1).likelihood(observations.get(0));
			backpointer[s][0] = 0;
		}

		// recursion
		for (int t = 1; t < numObs; t++) {
			T o_t = observations.get(t);
			for (int s = 1; s <= numStates; s++) {
				ObservationLikelihood<T> bs = b.get(s - 1);

				// take max over all possible previous states sp ("s prime")

				// Note: let sp start at 0 if want to allow starting from q_0 in the middle of the sequence
				// Note: let sp end at numStates + 1 (q_F) to allow for early stopping in middle of the sequence; but
				// with no observation???

				double bestScore = -1.0;
				int bestPointer = -1;
				for (int sp = 1; sp <= numStates; sp++) {
					double score = viterbi[sp][t - 1] * A[sp][s] * bs.likelihood(o_t);
					if (score > bestScore) {
						bestScore = score;
						bestPointer = sp;
					}
				}

				viterbi[s][t] = bestScore;
				backpointer[s][t] = bestPointer;
			}
		}

		// termination
		double bestScore = -1.0;
		int bestPointer = -1;

		for (int s = 1; s <= numStates; s++) {
			double score = viterbi[s][numObs - 1] * A[s][q_F];
			if (score > bestScore) {
				bestScore = score;
				bestPointer = s;
			}
		}

		viterbi[q_F][numObs - 1] = bestScore;
		backpointer[q_F][numObs - 1] = bestPointer;

		// pack answer into result object

		List<Integer> states = new ArrayList<Integer>();
		int state = backpointer[q_F][numObs - 1];
		states.add(state);
		for (int t = numObs - 1; t > 0; t--) {
			state = backpointer[state][t];
			states.add(state);
		}
		Collections.reverse(states);

		double score = viterbi[q_F][numObs - 1];

		StateSequence sequence = new StateSequence(states, score, 0);

		return sequence;
	}

	public void decodeLog(List<T> observations) {
	}

	private void updateTopK(List<StateSequence> sequences, StateSequence sequence) {
		int k = sequences.size();
		// replace last
		sequences.set(k - 1, sequence);
		// swap up list if necessary
		for (int i = k - 2; i >= 0; i--) {
			StateSequence si = sequences.get(i);
			StateSequence sj = sequences.get(i + 1);
			if (sj.getScore() > si.getScore()) {
				sequences.set(i, sj);
				sequences.set(i + 1, si);
			}
		}
	}

	/**
	 * Computes probability of going to q_F after observing o_t at time t. 
	 * 
	 * @param viterbi
	 * @param backpointer
	 * @param start
	 * @param t
	 * @return true if it was possible to transition to q_F after at being in some state at time t; false otherwise
	 */
	private boolean updateTrellisWithTransitionToEndState(double[][] viterbi, int[][] backpointer, int[][] start, int t) {
		boolean couldTransitionToEndState = false;

		double bestScore = -1.0;
		int bestPointer = -1;
		int bestStart = -1;
		for (int s = 1; s <= numStates; s++) {
			double score = viterbi[s][t] * A[s][q_F];
			if (score > bestScore) {
				bestScore = score;
				bestPointer = s;
				bestStart = start[s][t];
			}
		}

		if (bestScore > 0.0) {
			couldTransitionToEndState = true;
			viterbi[q_F][t] = bestScore;
			backpointer[q_F][t] = bestPointer;
			start[q_F][t] = bestStart;
		}

		return couldTransitionToEndState;
	}
	
	/**
	 * Extracts a sequence that transitioned from some state at time t to q_F at time t (with no observation).
	 */
	private StateSequence extractSequence(double[][] viterbi, int[][] backpointer, int[][] start, int endTime) {
		List<Integer> states = new ArrayList<Integer>();
		int state = backpointer[q_F][endTime];
		states.add(state);
		for (int t = endTime; t > 0; t--) {
			state = backpointer[state][t];
			if (state == q_0) {
				break;
			}
			states.add(state);
		}
		Collections.reverse(states);
		StateSequence sequence = new StateSequence(states, viterbi[q_F][endTime], start[q_F][endTime]);
		return sequence;
	}

	/**
	 * Finds top k subsequences of hidden states.
	 * 
	 * Follows pseudo-code from Jurafsky and Martin 2009, p. 186.
	 */
	public List<StateSequence> alignTopK(List<T> observations, int k) {
		// Note: pseudo-code goes from t = 1, ..., T, while this code goes from t = 0, ..., (numObs - 1).
		// Note: emission probability likelihoods are 0-indexed in b list, so need to subtract 1 from state index

		int numObs = observations.size();

		// viterbi(s, t) = probability of of most probable path ending in state s at time t 
		// given observations o_1, ..., o_t
		double[][] viterbi = new double[numStates + 2][numObs];

		// backpointer(s, t) = state at time t-1 for most probable path ending in state s at time t given observations
		int[][] backpointer = new int[numStates + 2][numObs];

		// start(s, t) = start time for most probable path ending in state s at time t, 
		// given that prefix of observation list can be truncated
		int[][] start = new int[numStates + 2][numObs];

		// store top k subsequences (initialize with dummy sequences)
		List<StateSequence> sequences = new ArrayList<StateSequence>();
		for (int i = 0; i < k; i++) {
			sequences.add(new StateSequence(null, Double.NEGATIVE_INFINITY, -1));
		}

		//
		// initialization (compute probability of going from q_0 to each state and observing o_0)
		//

		T o_0 = observations.get(0);
		for (int s = 1; s <= numStates; s++) {
			viterbi[s][0] = A[q_0][s] * b.get(s - 1).likelihood(o_0);
			backpointer[s][0] = 0;
			start[s][0] = 0;
		}

		// check for (early) finish
		int t = 0;
		if (k > 1) {
			if (updateTrellisWithTransitionToEndState(viterbi, backpointer, start, t)) {
				StateSequence sequence = extractSequence(viterbi,backpointer, start, t);
				updateTopK(sequences, sequence);
			}
		}

		//
		// recursion
		//

		for (t = 1; t < numObs; t++) {
			T o_t = observations.get(t);
			for (int s = 1; s <= numStates; s++) {
				ObservationLikelihood<T> bs = b.get(s - 1);

				// take max over all possible previous states sp ("s prime")
				double bestScore = -1.0;
				int bestPointer = -1;
				int bestStart = -1;
				for (int sp = 1; sp <= numStates; sp++) {
					double score = viterbi[sp][t - 1] * A[sp][s] * bs.likelihood(o_t);
					if (score > bestScore) {
						bestScore = score;
						bestPointer = sp;
						bestStart = start[sp][t - 1];
					}
				}

				// check for new start
				if (k > 1) {
					double score = A[q_0][s] * bs.likelihood(o_t);
					if (score > bestScore) {
						bestScore = score;
						bestPointer = 0;
						bestStart = t;
					}
				}

				viterbi[s][t] = bestScore;
				backpointer[s][t] = bestPointer;
				start[s][t] = bestStart;
			}

			// check for (early) finish
			// Note: this takes care of normal termination, too
			if (k > 1 || t == numObs - 1) {
				if (updateTrellisWithTransitionToEndState(viterbi, backpointer, start, t)) {
					StateSequence sequence = extractSequence(viterbi,backpointer, start, t);
					updateTopK(sequences, sequence);
				}
			}
		}

		return sequences;
	}
}
