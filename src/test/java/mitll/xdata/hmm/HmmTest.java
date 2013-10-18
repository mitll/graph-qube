package mitll.xdata.hmm;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HmmTest {
	private static Logger logger = Logger.getLogger(HmmTest.class);

	public static Hmm<DiscreteObservation> makeHotColdHmm() {
		// adapted from example in "Speech and Language Processing" (2nd ed) by Jurafsky and Martin, p. 178

		// two actual states plus start and end states
		// Note: both state 1 (hot) and state 2 (cold) are accepting/final states
		double[][] A = new double[4][];
		A[0] = new double[] { 0.0, 0.8, 0.2, 0.0 };
		A[1] = new double[] { 0.0, 0.7, 0.3, 1.0 };
		A[2] = new double[] { 0.0, 0.4, 0.6, 1.0 };
		A[3] = new double[] { 0.0, 0.0, 0.0, 0.0 };

		// probability of eating 1, 2, or 3 ice creams
		List<ObservationLikelihood<DiscreteObservation>> b = new ArrayList<ObservationLikelihood<DiscreteObservation>>();
		b.add(new DiscreteObservationLikelihood(new double[] { 0.2, 0.4, 0.4 })); // "Hot"
		b.add(new DiscreteObservationLikelihood(new double[] { 0.5, 0.4, 0.1 })); // "Cold"

		Hmm<DiscreteObservation> hmm = new Hmm<DiscreteObservation>(A, b);

		return hmm;
	}

	public static List<DiscreteObservation> makeObservations(int... data) {
		List<DiscreteObservation> observations = new ArrayList<DiscreteObservation>();
		for (int i = 0; i < data.length; i++) {
			observations.add(new DiscreteObservation(data[i]));
		}
		return observations;
	}

	public static List<Integer> makeStates(int... data) {
		List<Integer> states = new ArrayList<Integer>();
		for (int i = 0; i < data.length; i++) {
			states.add(data[i]);
		}
		return states;
	}

	@Test
	public void testDiscreteViterbi() {
		logger.debug("ENTER testDiscreteViterbi()");

		Hmm<DiscreteObservation> hmm = makeHotColdHmm();

		// # ice cream events: 3, 1, 3 (becomes 2 0 2)
		// List<DiscreteObservation> observations = makeList(2, 0, 2);
		List<DiscreteObservation> observations = makeObservations(2, 2, 2, 0, 0, 0);

		StateSequence sequence = hmm.decodeTopK(observations, 1).get(0);

		logger.debug("score = " + sequence.getScore());
		logger.debug("states = " + sequence.getStates());

		Assert.assertEquals(sequence.getStates(), makeStates(1, 1, 1, 2, 2, 2));

		logger.debug("EXIT testDiscreteViterbi()");
	}

	@Test
	public void testDiscreteDecodeTopK() {
		logger.debug("ENTER testDiscreteDecodeTopK()");

		Hmm<DiscreteObservation> hmm = makeHotColdHmm();

		// # ice cream events: 3, 1, 3 (becomes 2 0 2)
		// List<DiscreteObservation> observations = makeList(2, 0, 2);
		List<DiscreteObservation> observations = makeObservations(2, 2, 2, 0, 0, 0);

		List<StateSequence> sequences = hmm.decodeTopK(observations, 10);

		logger.debug("sequences.size() = " + sequences.size());
		for (StateSequence sequence : sequences) {
			logger.debug("score = " + sequence.getScore() + "; states = " + sequence.getStates() + "; start = "
					+ sequence.getStartIndex());
		}

		// check sorted
		for (int i = 0; i < sequences.size() - 1; i++) {
			Assert.assertTrue(sequences.get(i).getScore() >= sequences.get(i + 1).getScore());
		}

		logger.debug("EXIT testDiscreteDecodeTopK()");
	}

	@Test
	public void testProbability() {
		logger.debug("ENTER testProbability()");

		Hmm<DiscreteObservation> hmm = makeHotColdHmm();

		List<Integer> states = makeStates(1, 1, 1, 2, 2, 2);
		List<DiscreteObservation> observations = makeObservations(2, 2, 2, 0, 0, 0);

		double p = hmm.probability(states, observations);
		logger.debug("p = " + p);

		p = hmm.probability(makeStates(1), makeObservations(2));
		logger.debug("p = " + p);

		p = hmm.probability(makeStates(1), makeObservations(0));
		logger.debug("p = " + p);

		logger.debug("EXIT testProbability()");
	}

	@Test
	public void testLeftToRightHmm() {
		logger.debug("ENTER testLeftToRightHmm()");

		double selfTransition = 0.25;

		double[][] A = new double[5][];
		A[0] = new double[] { 0.0, 1.0, 0.0, 0.0, 0.0 }; // q_0
		A[1] = new double[] { 0.0, selfTransition, 1.0 - selfTransition, 0.0, 0.0 }; // state 1
		A[2] = new double[] { 0.0, 0.0, selfTransition, 1.0 - selfTransition, 0.0 }; // state 2
		A[3] = new double[] { 0.0, 0.0, 0.0, selfTransition, 1.0 - selfTransition }; // state 3
		A[4] = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0 }; // q_F

		List<ObservationLikelihood<DiscreteObservation>> b = new ArrayList<ObservationLikelihood<DiscreteObservation>>();
		b.add(new DiscreteObservationLikelihood(new double[] { 0.8, 0.15, 0.05 })); // state 1 prefers "0"
		b.add(new DiscreteObservationLikelihood(new double[] { 0.05, 0.8, 0.15 })); // state 2 prefers "1"
		b.add(new DiscreteObservationLikelihood(new double[] { 0.15, 0.05, 0.8 })); // state 3 prefers "2"

		Hmm<DiscreteObservation> hmm = new Hmm<DiscreteObservation>(A, b);

		List<DiscreteObservation> observations = makeObservations(2, 0, 1, 0, 1, 2, 2, 0, 0, 1, 2);

		List<StateSequence> sequences = hmm.decodeTopK(observations, 10);

		logger.debug("sequences.size() = " + sequences.size());
		for (StateSequence sequence : sequences) {
			logger.debug("score = " + sequence.getScore() + "; states = " + sequence.getStates() + "; start = "
					+ sequence.getStartIndex());
		}

		logger.debug("EXIT testLeftToRightHmm()");
	}

	@Test
	public void testLeftToRightHmmLog() {
		logger.debug("ENTER testLeftToRightHmmLog()");

		double selfTransition = 0.25;

		double[][] A = new double[5][];
		A[0] = new double[] { 0.0, 1.0, 0.0, 0.0, 0.0 }; // q_0
		A[1] = new double[] { 0.0, selfTransition, 1.0 - selfTransition, 0.0, 0.0 }; // state 1
		A[2] = new double[] { 0.0, 0.0, selfTransition, 1.0 - selfTransition, 0.0 }; // state 2
		A[3] = new double[] { 0.0, 0.0, 0.0, selfTransition, 1.0 - selfTransition }; // state 3
		A[4] = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0 }; // q_F

		List<ObservationLikelihood<DiscreteObservation>> b = new ArrayList<ObservationLikelihood<DiscreteObservation>>();
		b.add(new DiscreteObservationLikelihood(new double[] { 0.8, 0.15, 0.05 })); // state 1 prefers "0"
		b.add(new DiscreteObservationLikelihood(new double[] { 0.05, 0.8, 0.15 })); // state 2 prefers "1"
		b.add(new DiscreteObservationLikelihood(new double[] { 0.15, 0.05, 0.8 })); // state 3 prefers "2"

		Hmm<DiscreteObservation> hmm = new Hmm<DiscreteObservation>(A, b);

		List<DiscreteObservation> observations = makeObservations(2, 0, 1, 0, 1, 2, 2, 0, 0, 1, 2);

		logger.debug("no log");
		List<StateSequence> sequences = hmm.decodeTopK(observations, 10);
		logger.debug("sequences.size() = " + sequences.size());
		for (StateSequence sequence : sequences) {
			logger.debug("score = " + sequence.getScore() + "; states = " + sequence.getStates() + "; start = "
					+ sequence.getStartIndex());
		}

		logger.debug("log");
		List<StateSequence> logSequences = hmm.decodeTopKLog(observations, 10);
		logger.debug("logSequences.size() = " + logSequences.size());
		for (StateSequence sequence : logSequences) {
			logger.debug("score = " + sequence.getScore() + "; states = " + sequence.getStates() + "; start = "
					+ sequence.getStartIndex());
		}
		
		double delta = 1e-6;
		for (int i = 0; i < sequences.size(); i++) {
			Assert.assertEquals(sequences.get(i).getScore(), Math.exp(logSequences.get(i).getScore()), delta);
		}

		logger.debug("EXIT testLeftToRightHmmLog()");
	}
}
