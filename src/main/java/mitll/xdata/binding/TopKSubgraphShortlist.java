package mitll.xdata.binding;

import influent.idl.*;
import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.db.DBConnection;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Uses the Top-K Subgraph Search algorithm in uiuc.topksubgraph
 * as shortlisting engine  
 * 
 * @author Charlie Dagli (dagli@ll.mit.edu)
 * MIT Lincoln Laboratory
 */
public class TopKSubgraphShortlist extends Shortlist {
  private static final Logger logger = Logger.getLogger(TopKSubgraphShortlist.class);

  private static final int DEFAULT_SHORT_LIST_SIZE = 100;
  private static final int MAX_CANDIDATES = 100;
  private static final long MB = 1024*1024;
  private static final int FULL_SEARCH_LIST_SIZE = 200;
  private static final int MAX_TRIES = 1000000;
  
  private static Connection connection;
  private static PreparedStatement queryStatement;

  public TopKSubgraphShortlist(Binding binding) {
	  super(binding);
  }

  public List<FL_PatternSearchResult> getShortlist(Connection connectionIn, 
		  List<FL_EntityMatchDescriptor> entities1, 
		  List<String> exemplarIDs, long max) {

	  connection = connectionIn;

	  //check to see if we can connect to anything....
	  if (existsTable("MARGINAL_GRAPH")) {
		  logger.info("we can connect to the right database...");
	  } else {
		  logger.info("table MARGINAL_GRAPH does not yet exist.");
	  }

	  /*
	   * Get all pairs of query nodes...
	   * (this is assuming ids are sortable by integer comparison, like in bitcoin)
	   */
	  int e1, e2;
	  if (exemplarIDs.size() > 1) {
		  for (int i=0;i<exemplarIDs.size();i++){
			  for (int j=i+1;j<exemplarIDs.size();j++) {
				  e1 = Integer.parseInt(exemplarIDs.get(i));
				  e2 = Integer.parseInt(exemplarIDs.get(j));
				  if (e1 <= e2) {
					  logger.info("pair: ("+e1+", "+e2+")"); 
				  } else {
					  logger.info("pair: ("+e2+", "+e1+")"); 
				  }
			  }  
		  }	  
	  }
	
	  int k = (int) (max/1);
	  boolean skipSelf = SKIP_SELF_AS_NEIGHBOR;

	  long then = System.currentTimeMillis();
	  SortedSet<CandidateGraph> candidates = new TreeSet<CandidateGraph>();
	  String firstExemplar = null;
	  if (!exemplarIDs.isEmpty()) {
		  firstExemplar = exemplarIDs.iterator().next();
		  candidates = getCandidateGraphs(exemplarIDs, k, skipSelf, firstExemplar);
	  }

	  if (!candidates.isEmpty()) {
		  logger.debug("getShortlistFast : " + candidates.size() + " best " + candidates.first() + " worst " + candidates.last());
	  }

	  int count = 0;
	  CandidateGraph queryGraph = new CandidateGraph(binding, exemplarIDs, firstExemplar, 10).makeDefault();
	  boolean found = false;
	  for (CandidateGraph graph : candidates) {
		  if (graph.equals(queryGraph)) {
			  found = true;
			  break;
		  }
	  }
	  if (!found) candidates.add(queryGraph);

	  List<FL_PatternSearchResult> results = getPatternSearchResults(entities1, exemplarIDs, candidates, queryGraph);

	  long now = System.currentTimeMillis();
	  logger.debug("getShortlistFast took " + (now - then) + " millis to get " + results.size() + " candidates");

	  return results;
  }


  /**
   * @param connection
   * @param tableName
   */
  private static boolean existsTable(String tableName) {
	  try {
		  String sql = "select 1 from "+tableName+" limit 1;";
		  ResultSet rs = doSQLQuery(sql);
		  rs.close();
		  queryStatement.close();

		  return true;
	  } catch (SQLException e) {
		  return false;
	  }
  }
  
	/**
	 * Do SQL, update something, return nothing
	 * 
	 * @param createSQL
	 * @throws SQLException
	 */
	private static void doSQLUpdate(String sql) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.executeUpdate();
		statement.close();
	}

	/**
	 * Do SQL, query something, return that something
	 * 
	 * @param createSQL
	 * @return
	 * @throws SQLException
	 */
	private static ResultSet doSQLQuery(String sql) throws SQLException {
		queryStatement = connection.prepareStatement(sql);
		ResultSet rs = queryStatement.executeQuery();
		
		return rs;
	}	
	
	
	
	

  /**
   * Convert candidate graphs into pattern search results.
   * @param entities1
   * @param exemplarIDs
   * @param candidates
   * @param queryGraph
   * @return
   */
  private List<FL_PatternSearchResult> getPatternSearchResults(List<FL_EntityMatchDescriptor> entities1,
                                                               List<String> exemplarIDs,
                                                               Collection<CandidateGraph> candidates, CandidateGraph queryGraph) {
    List<FL_PatternSearchResult> results = new ArrayList<FL_PatternSearchResult>();

    for (CandidateGraph graph : candidates) {
      List<FL_EntityMatchResult> entities = new ArrayList<FL_EntityMatchResult>();

      List<String> nodes = graph.getNodes();
      for (int i = 0; i < exemplarIDs.size(); i++) {
        String similarID = nodes.get(i);
        double similarity = binding.getSimilarity(exemplarIDs.get(i), similarID);
        String exemplarQueryID = entities1.get(i).getUid();
        if (queryGraph.getScore() > ((float) exemplarIDs.size()) - 0.1) {
          logger.debug("\t graph " + graph + " got " + similarID + " and " + exemplarIDs.get(i));
        }
        FL_EntityMatchResult entityMatchResult = binding.makeEntityMatchResult(exemplarQueryID, similarID, similarity);
        if (entityMatchResult != null) {
          entities.add(entityMatchResult);
        }
      }

      // arithmetic mean
      double score = getSimpleScore(entities);
      boolean query = graph == queryGraph;
      FL_PatternSearchResult result = makeResult(entities, score, query);
      if (query) {
        logger.debug("found query!!! " + result);
      }
      results.add(result);
    }
    return results;
  }


  /**
   * @see #getShortlist(java.util.List, java.util.List, long)
   * @param exemplarIDs
   * @param k
   * @param skipSelf
   * @param firstExemplar
   * @return
   */
  private SortedSet<CandidateGraph> getCandidateGraphs(List<String> exemplarIDs, int k, boolean skipSelf, String firstExemplar) {
    SortedSet<CandidateGraph> candidates;
    candidates = new TreeSet<CandidateGraph>();

    List<String> neighbors = binding.getNearestNeighbors(firstExemplar, k, skipSelf);
    logger.debug("for " + firstExemplar + " found " + neighbors.size() + " neighbors with k " + k + " stot " +binding.getNumSourceNodes() + " exemplars " + exemplarIDs);
    //  candidates.add(new CandidateGraph(exemplarIDs, firstExemplar, k));

    // for each neighbor, make a one-node graph
    for (String node : neighbors) {
      if (binding.isNodeId(node)) {
        candidates.add(new CandidateGraph(binding, exemplarIDs, node, k));
      }
    }

    if (!candidates.isEmpty()) {
      logger.debug("depth 1 : " + candidates.size() + " best " + candidates.first() + " worst " + candidates.last());
    } else {
      logger.debug("depth 1 NO CANDIDATES");
    }

    // create candidate graphs with as many nodes as the exemplar ids
    for (int i = 1; i < exemplarIDs.size(); i++) {
      logger.debug("exemplar  #" + i);

      SortedSet<CandidateGraph> nextCandidates = new TreeSet<CandidateGraph>();
      for (CandidateGraph candidateGraph : candidates) {
        candidateGraph.makeNextGraphs2(nextCandidates, MAX_CANDIDATES);

      /*  if (!nextCandidates.isEmpty()) {
          logger.debug("1 depth " + i +
              " : " + nextCandidates.size() + " best " + nextCandidates.first() + " worst " + nextCandidates.last());
        }*/
      }

      candidates = nextCandidates;
      if (!candidates.isEmpty()) {
        logger.debug("2 depth " + i +
            " : " + candidates.size() + " best " + candidates.first() + " worst " + candidates.last());
      }
    }

    logger.debug("returning " + candidates.size());
    return candidates;
  }


}
