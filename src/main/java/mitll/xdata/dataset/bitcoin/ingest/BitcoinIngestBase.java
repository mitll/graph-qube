package mitll.xdata.dataset.bitcoin.ingest;

import mitll.xdata.binding.Binding;
import org.apache.log4j.Logger;

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
  void doSubgraphs(String dbName, Set<Integer> entityIds) throws Throwable {
    long then;
    /*
		 * Pre-processing the transaction data to prepare it for topk-subgraph search:
		 * - Graph construction, filtering and indexing
		 */
    then = System.currentTimeMillis();

    // Filter-out non-active nodes, self-transitions, heavy-hitters
    BitcoinIngestSubGraph.filterForActivity("h2", dbName);

    Binding.logMemory();
    // Create marginalized graph data and various stats
    Map<Long, Integer> edgeToWeight = BitcoinIngestSubGraph.extractUndirectedGraphInMemory("h2", dbName, entityIds);

    Binding.logMemory();

    //Do the indexing for the topk-subgraph algorithm
    //BitcoinIngestSubGraph.computeIndices("h2", dbName);

    BitcoinIngestSubGraph.computeIndicesFromMemory("h2", dbName, edgeToWeight);

    Binding.logMemory();

    long now = System.currentTimeMillis();
    logger.debug("SubGraph Search Ingest (graph building, filtering, index construction) complete. Elapsed time: " +
        (now - then) / 1000 + " seconds");
  }
}
