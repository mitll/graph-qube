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

package mitll.xdata.binding;

import influent.idl.FL_LinkMatchResult;
import influent.idl.FL_PatternSearchResult;

import java.util.ArrayList;
import java.util.List;

public class PatternSearchResultWithState extends FL_PatternSearchResult {
	private List<List<FL_LinkMatchResult>> phaseLinks = new ArrayList<List<FL_LinkMatchResult>>();
	private List<String> states = new ArrayList<String>();

  private boolean isQuery;
  /**
   * @see Binding#makeResult
   */
	public PatternSearchResultWithState(boolean query) {
		super();
    this.isQuery = query;
	}

	public List<List<FL_LinkMatchResult>> getPhaseLinks() {
		return phaseLinks;
	}

	public List<String> getStates() {
		return states;
	}

  public String toString() {
    return "search result , query " + isQuery + " links " + phaseLinks.size() + " states " + states.size();
  }
}
