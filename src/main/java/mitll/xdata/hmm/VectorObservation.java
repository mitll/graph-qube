package mitll.xdata.hmm;

import java.util.List;

import mitll.xdata.binding.Binding.Edge;

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
