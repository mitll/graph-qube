package mitll.xdata.dataset.bitcoin.ingest;

import org.apache.log4j.Logger;

/**
 * Created by go22670 on 8/6/15.
 */
public class BitcoinIngestBase {
	private static final Logger logger = Logger.getLogger(BitcoinIngestBase.class);

	/**
	 * @see BitcoinIngestUncharted#doIngest(String, String, String, String, boolean, long)
	 * @param dbName
	 * @throws Throwable
   */
	void doSubgraphs(String dbName) throws Throwable {
		long then;
		/*
		 * Pre-processing the transaction data to prepare it for topk-subgraph search:
		 * - Graph construction, filtering and indexing
		 */
		then = System.currentTimeMillis();

		// Filter-out non-active nodes, self-transitions, heavy-hitters
		BitcoinIngestSubGraph.filterForActivity("h2", dbName);

		// Create marginalized graph data and various stats
		BitcoinIngestSubGraph.extractUndirectedGraph("h2",dbName);

		//Do the indexing for the topk-subgraph algorithm
		BitcoinIngestSubGraph.computeIndices("h2", dbName);

		long now = System.currentTimeMillis();
		logger.debug("SubGraph Search Ingest (graph building, filtering, index construction) complete. Elapsed time: " +(now-then)/1000 + " seconds");
	}
}
