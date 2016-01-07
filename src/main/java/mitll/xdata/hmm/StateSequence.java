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
