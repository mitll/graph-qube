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

package mitll.xdata.dataset.bitcoin.ingest;

import mitll.xdata.binding.Binding;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by go22670 on 8/6/15.
 */
public class BitcoinIngestBase {
  private static final Logger logger = Logger.getLogger(BitcoinIngestBase.class);

  /**
   * @param dbName
   * @throws Throwable
   * @see BitcoinIngestUncharted#doIngest(String, String, String, String, boolean, long)
   */
  Set<Integer> doSubgraphs(String dbName, Collection<Integer> entityIds) throws Throwable {
    long then;
    /*
     * Pre-processing the transaction data to prepare it for topk-subgraph search:
		 * - Graph construction, filtering and indexing
		 */
    then = System.currentTimeMillis();

    // Filter-out non-active nodes, self-transitions, heavy-hitters
    String h2 = "h2";
    BitcoinIngestSubGraph.filterForActivity(h2, dbName);

    Binding.logMemory();
    // Create marginalized graph data and various stats

    logger.info("doSubgraphs " +entityIds.size());
    Map<Long, Integer> edgeToWeight = BitcoinIngestSubGraph.extractUndirectedGraphInMemory(h2, dbName, entityIds);

    Binding.logMemory();

    Set<Integer> uniqueids = BitcoinIngestSubGraph.makeMarginalGraph(h2, dbName, edgeToWeight);
    //Do the indexing for the topk-subgraph algorithm
    //BitcoinIngestSubGraph.computeIndices("h2", dbName);

    BitcoinIngestSubGraph.computeIndicesFromMemory(h2, dbName, edgeToWeight);

    Binding.logMemory();

    long now = System.currentTimeMillis();
    logger.debug("SubGraph Search Ingest (graph building, filtering, index construction) complete. Elapsed time: " +
        (now - then) / 1000 + " seconds");

    return uniqueids;
  }
}
