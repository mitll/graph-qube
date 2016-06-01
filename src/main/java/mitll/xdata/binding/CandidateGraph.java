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

import org.apache.log4j.Logger;

import java.util.*;

/**
* Created by go22670 on 4/10/14.
*/
class CandidateGraph implements Comparable<CandidateGraph> {
  private static final Logger logger = Logger.getLogger(CandidateGraph.class);

  private static final int MAX_NEIGHBORS = 1000;
  private Binding binding;
  private final List<String> exemplars;
  private List<String> nodes = new ArrayList<String>();
  private float score;
  final int k;

  /**
   *
   * @param binding
   * @param toCopy
   */
  private CandidateGraph(Binding binding, CandidateGraph toCopy) {
    this.binding = binding;
    exemplars = toCopy.exemplars;
    nodes = new ArrayList<String>(toCopy.getNodes());
    this.k = toCopy.k;
    this.score = toCopy.score;
  }

  /**
   * @see Binding#getCandidateGraphs(java.util.List, int, boolean, String)
   * @see mitll.xdata.binding.Binding#getShortlistFast(java.util.List, java.util.List, long)
   * @param binding
   * @param exemplars
   * @param initial
   * @param k
   */
  CandidateGraph(Binding binding, List<String> exemplars, String initial, int k) {
    this.binding = binding;
    this.exemplars = exemplars;
    addNode(initial);
    this.k = k;
  }

  CandidateGraph makeDefault() {
    for (String exemplar : exemplars){
      if (!nodes.contains(exemplar)) nodes.add(exemplar);
    }
    return this;
  }
/*    public boolean wouldBeConnected(String nodeid) {
    boolean connected = false;
    try {
      for (String currentNode : nodes) {
        connected = isPairConnected(currentNode, nodeid);
        if (connected) break;
      }
    } catch (Exception e) {
    }
    return connected;
  }*/
/*    private List<CandidateGraph> makeNextGraphs() {
    List<String> neighbors = getNearestNeighbors(nodes.get(nodes.size() - 1), k, SKIP_SELF_AS_NEIGHBOR);

    List<CandidateGraph> nextHopGraphs = new ArrayList<CandidateGraph>();
    for (String nextHopNode : neighbors) {
      if (!nodes.contains(nextHopNode) && wouldBeConnected(nextHopNode) && validTargets.contains(nextHopNode)) {
        CandidateGraph candidateGraph = new CandidateGraph(this);
        candidateGraph.addNode(nextHopNode);
        nextHopGraphs.add(candidateGraph);
      }
    }
    return nextHopGraphs;
  }*/

  /**
   * Find immediate neighbors of the last node added to the graph
   *
   * @param candidates
   * @param maxSize
   */
  void makeNextGraphs2(SortedSet<CandidateGraph> candidates, int maxSize) {
    final String lastNodeInGraph = nodes.get(nodes.size() - 1);
    Set<String> oneHopNeighbors = binding.stot.get(lastNodeInGraph);
    if (oneHopNeighbors == null) {
      logger.error("huh? '" + lastNodeInGraph + "' has no transactions?");
      return;
    }
    List<String> sortedNeighbors = new ArrayList<String>(oneHopNeighbors);
    Collections.sort(sortedNeighbors, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        double toFirst = binding.getSimilarity(lastNodeInGraph, o1);
        double toSecond = binding.getSimilarity(lastNodeInGraph, o2);
        return toFirst < toSecond ? +1 : toFirst > toSecond ? -1 : 0;
      }
    });

    sortedNeighbors = sortedNeighbors.subList(0, Math.min(sortedNeighbors.size(), MAX_NEIGHBORS));

    //logger.debug("this " + this + " found " + sortedNeighbors.size() + " neighbors of " + lastNodeInGraph);
    for (String nextHopNode : sortedNeighbors) {
      if (!nodes.contains(nextHopNode) && binding.validTargets.contains(nextHopNode)) {    // no cycles!
        CandidateGraph candidateGraph = new CandidateGraph(binding,this);
        candidateGraph.addNode(nextHopNode);
        if (candidates.size() < maxSize || candidateGraph.getScore() > candidates.last().getScore()) {
          if (candidates.size() == maxSize) {
            candidates.remove(candidates.last());
            //logger.debug("this " + this + " removing " + candidates.last()+ " and adding " + candidateGraph);

          }
          candidates.add(candidateGraph);
        }
      }
    }
  }

  /**
   * Compare the node to add against the last node and increment the score.
   * @param nodeid
   */
  private void addNode(String nodeid) {
    String compareAgainst = exemplars.get(getNodes().size());
    double similarity = binding.getSimilarity(compareAgainst, nodeid);
    score += similarity;
    nodes.add(nodeid);
  }

  @Override
  public int compareTo(CandidateGraph o) {
    return o.getScore() < getScore() ? -1 : o.getScore() > getScore() ? +1 : 0;
  }

  public List<String> getNodes() {
    return nodes;
  }

  public float getScore() {
    return score;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CandidateGraph) {
      CandidateGraph o = (CandidateGraph) obj;
      return nodes.equals(o.getNodes());
    }
    else return false;
  }

  public String toString() {
    boolean isQuery = getNodes().equals(exemplars);
     return (isQuery ? " QUERY " : "")+" nodes " + getNodes() + " score " + getScore();
  }
}
