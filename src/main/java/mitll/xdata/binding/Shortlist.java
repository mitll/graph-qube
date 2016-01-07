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

import influent.idl.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by go22670 on 4/10/14.
 */
public abstract class Shortlist {
	protected static final boolean SKIP_SELF_AS_NEIGHBOR = false;
	final Binding binding;

	protected int numQueries = 0;

	public Shortlist(Binding binding) {
		this.binding = binding;
	}
	
	protected double getSimpleScore(List<FL_EntityMatchResult> entities) {
		double sum = 0.0;
		for (FL_EntityMatchResult entityMatchResult : entities) {
			sum += entityMatchResult.getScore();
		}
		return sum / entities.size();
	}

	/**
	 * @see mitll.xdata.binding.BreadthFirstShortlist#getPatternSearchResults(java.util.List, java.util.List, java.util.Collection, CandidateGraph)
	 * @param entities
	 * @param score
	 * @param isQuery
	 * @return
	 */
	protected FL_PatternSearchResult makeResult(List<FL_EntityMatchResult> entities, double score, boolean isQuery) {
		FL_PatternSearchResult result = new PatternSearchResultWithState(isQuery);
		result.setEntities(entities);
		List<FL_LinkMatchResult> links = getLinks(entities);
		result.setLinks(links);
		result.setScore(score);
		return result;
	}

	/**
	 * @return links between entities (if connected in edge_index table)
	 * @see #makeResult(java.util.List, double, boolean)
	 */
	private List<FL_LinkMatchResult> getLinks(List<FL_EntityMatchResult> entities) {
		// NOTE : this returns at most one link per pair of nodes...

		List<FL_LinkMatchResult> linkMatchResults = new ArrayList<FL_LinkMatchResult>();

		if (entities.size() == 1) {
			return linkMatchResults;
		}

		try {
			// iterate over all pairs of entities
			for (int i = 0; i < entities.size(); i++) {
				String source = entities.get(i).getEntity().getUid();
				for (int j = i + 1; j < entities.size(); j++) {
					String target = entities.get(j).getEntity().getUid();
					String edgeMetadataKey = binding.getEdgeMetadataKey(source, target);
					if (edgeMetadataKey != null) {
						FL_LinkMatchResult linkMatchResult = new FL_LinkMatchResult();
						FL_Link link = new FL_Link();
						link.setSource(source);
						link.setTarget(target);
						//link.setTags(new ArrayList<FL_LinkTag>()); //deprecated in Influent IDL 2.0
						List<FL_Property> properties = new ArrayList<FL_Property>();
						properties.add(binding.createEdgeMetadataKeyProperty(edgeMetadataKey));
						link.setProperties(properties);
						linkMatchResult.setLink(link);
						linkMatchResult.setScore(1.0);
						linkMatchResult.setUid("");
						linkMatchResults.add(linkMatchResult);
					} else {
						// System.out.println("no edge between: " + source + " & " + target);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<FL_LinkMatchResult>();
		}

		return linkMatchResults;
	}

	/**
	 * @return FL_Property for edge metadata key (key into table with additional edge attributes)
	 */
	protected abstract List<FL_PatternSearchResult> getShortlist(List<FL_EntityMatchDescriptor> entities1, 
			List<String> exemplarIDs, long max);
}
