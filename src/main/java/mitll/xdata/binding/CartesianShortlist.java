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

import influent.idl.FL_EntityMatchDescriptor;
import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_PatternSearchResult;
import mitll.xdata.PrioritizedCartesianProduct;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by go22670 on 4/10/14.
 */
public class CartesianShortlist extends Shortlist {
  private static final Logger logger = Logger.getLogger(CartesianShortlist.class);
  private static final int MAX_TRIES = 1000000;
  private static final boolean SKIP_SELF_AS_NEIGHBOR = false;

  public CartesianShortlist(Binding binding) {
    super(binding);
  }

  /**
   * @see Binding#getShortlist(influent.idl.FL_PatternDescriptor, long)
   * @param entities1
   * @param exemplarIDs
   * @param max
   * @return
   * @deprecated
   */
  public List<FL_PatternSearchResult> getShortlist(List<FL_EntityMatchDescriptor> entities1, List<String> exemplarIDs,
                                                    long max) {
    long then = System.currentTimeMillis();
    // TODO : skip including self in list?
    boolean skipSelf = SKIP_SELF_AS_NEIGHBOR;
    Map<String, List<String>> idToNeighbors = new HashMap<String, List<String>>();

    int k = (int) (100 * max);
    for (String id : exemplarIDs) {
      List<String> neighbors = binding.getNearestNeighbors(id, k, skipSelf);
      if (neighbors.size() == 0) {
        logger.warn("no neighbors for " + id + " among nearest " +k+ "???\n\n");
        return Collections.emptyList();
      }
      idToNeighbors.put(id, neighbors);
    }

    logger.debug("for " + exemplarIDs.size() + " examples, took " + (System.currentTimeMillis() - then)
        + " millis to find neighbors");

    int[] listSizes = new int[exemplarIDs.size()];
    for (int i = 0; i < exemplarIDs.size(); i++) {
      listSizes[i] = idToNeighbors.get(exemplarIDs.get(i)).size();
    }
    PrioritizedCartesianProduct product = new PrioritizedCartesianProduct(listSizes);

    long maxTries = max + MAX_TRIES;
    long numTries = 0;
    int count = 0;
    List<FL_PatternSearchResult> results = new ArrayList<FL_PatternSearchResult>();

    // TODO : grab more than max since we're going to re-sort with a refined subgraph score?
    while (product.next()) {
      numTries++;

      if (numTries % 100000 == 0) {
        logger.debug("numTries = " + numTries + " / maxTries = " + maxTries);
      }

      if (numTries >= maxTries) {
        logger.debug("reached maxTries = " + maxTries);
        break;
      }

      int[] indices = product.getIndices();

      List<String> subgraphIDs = new ArrayList<String>();
      for (int i = 0; i < indices.length; i++) {
        String exemplarID = exemplarIDs.get(i);
        String similarID = idToNeighbors.get(exemplarID).get(indices[i]);
        subgraphIDs.add(similarID);
      }

      // skip if any node id repeated
      Set<String> idSet = new HashSet<String>(subgraphIDs);
      if (idSet.size() < subgraphIDs.size()) {
        continue;
      }

      // skip if not somewhat connected
      if (!connectedGroup(subgraphIDs)) {
        continue;
      }

      List<FL_EntityMatchResult> entities = getEntitiesFromIds(entities1, exemplarIDs, idToNeighbors, indices);

      // arithmetic mean
      double score = getSimpleScore(entities);
      FL_PatternSearchResult result = makeResult(entities, score, false);
      results.add(result);

      count++;
      if (count >= max) {
        break;
      }
    }

    //logger.debug("numTries = " + numTries + " results " + results.size());

    return results;
  }


  /**
   * @see Binding#searchByExample(influent.idl.FL_PatternDescriptor, String, long, long, boolean)
   * @return true if each node connected to at least one other node
   */
  private boolean connectedGroup(List<String> ids) {
    // TODO : verify that group is actually connected?

    //long then = System.currentTimeMillis();

    if (ids.size() == 1) {
      return true;
    }

    try {
      for (int i = 0; i < ids.size(); i++) {
        boolean connected = false;
        for (int j = 0; j < ids.size(); j++) {
          if (j == i) {
            continue;
          }
          connected = binding.isPairConnected(ids.get(i), ids.get(j));
          if (connected) {
            break;
          }
        }
        if (!connected) {
          return false;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }

    // logger.debug("connected : took "+ (System.currentTimeMillis()-then) + " millis to check " +ids.size());
    return true;
  }



  private List<FL_EntityMatchResult> getEntitiesFromIds(List<FL_EntityMatchDescriptor> entities1,
                                                        List<String> exemplarIDs,
                                                        Map<String, List<String>> idToNeighbors, int[] indices) {
    List<FL_EntityMatchResult> entities = new ArrayList<FL_EntityMatchResult>();
    for (int i = 0; i < indices.length; i++) {
      String exemplarQueryID = entities1.get(i).getUid();

      String exemplarID = exemplarIDs.get(i);
      String similarID = idToNeighbors.get(exemplarID).get(indices[i]);
      double similarity = binding.getSimilarity(exemplarID, similarID);
      FL_EntityMatchResult entityMatchResult = binding.makeEntityMatchResult(exemplarQueryID, similarID, similarity);
      if (entityMatchResult != null) {
        entities.add(entityMatchResult);
      }
    }
    return entities;
  }


}
