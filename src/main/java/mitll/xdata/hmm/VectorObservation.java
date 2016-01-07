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

import mitll.xdata.binding.Binding.Edge;

import java.util.List;

/**
 * @see mitll.xdata.binding.Binding#addRelevantEdges(influent.idl.FL_PatternSearchResult, java.util.List)
 */
public class VectorObservation implements Observation {
	private double[] values;
	
	// TODO: probably shouldn't be stored here (or at least should be more generic, e.g., "data")
	private List<Edge> edges;
	
	// TODO: probably shouldn't be stored here?
	private String state;
	
	public VectorObservation(double[] values) {
		this.values = values;
	}

	public double[] getValues() {
		return values;
	}

	public void setValues(double[] values) {
		this.values = values;
	}

	public List<Edge> getEdges() {
		return edges;
	}

	public void setEdges(List<Edge> edges) {
		this.edges = edges;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}
}
