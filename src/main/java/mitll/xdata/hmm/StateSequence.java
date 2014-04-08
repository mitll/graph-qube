package mitll.xdata.hmm;

import java.util.List;

/**
 * @see mitll.xdata.binding.Binding#rescoreWithHMM(java.util.List, java.util.List, java.util.List, java.util.List, java.util.List)
 */
public class StateSequence {
	/** sequence of states for this subsequence */
	private List<Integer> states;

	/** score for this subsequence */
	private double score;

	/** index into observations list for this subsequence */
	private int startIndex;
	
	public StateSequence(List<Integer> states, double score, int startIndex) {
		this.states = states;
		this.score = score;
		this.startIndex = startIndex;
	}

	public List<Integer> getStates() {
		return states;
	}

	public void setStates(List<Integer> states) {
		this.states = states;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}
}
