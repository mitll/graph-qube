package mitll.xdata.binding;

import influent.idl.FL_LinkMatchResult;
import influent.idl.FL_PatternSearchResult;

import java.util.ArrayList;
import java.util.List;

public class PatternSearchResultWithState extends FL_PatternSearchResult {
	private List<List<FL_LinkMatchResult>> phaseLinks = new ArrayList<List<FL_LinkMatchResult>>();
	private List<String> states = new ArrayList<String>();

  public boolean isQuery;
  /**
   * @see Binding#makeResult(java.util.List, double)
   */
	public PatternSearchResultWithState(boolean query) {
		super();
    this.isQuery = query;
	}

	public List<List<FL_LinkMatchResult>> getPhaseLinks() {
		return phaseLinks;
	}

	public void setPhaseLinks(List<List<FL_LinkMatchResult>> phaseLinks) {
		this.phaseLinks = phaseLinks;
	}

	public List<String> getStates() {
		return states;
	}

	public void setStates(List<String> states) {
		this.states = states;
	}

  public String toString() { return "search result , query " + isQuery + " links " + phaseLinks.size() + " states " + states.size();}
}
