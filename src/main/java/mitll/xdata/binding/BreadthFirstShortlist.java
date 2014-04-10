package mitll.xdata.binding;

import influent.idl.*;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by go22670 on 4/10/14.
 */
public class BreadthFirstShortlist extends Shortlist {
  private static final Logger logger = Logger.getLogger(BreadthFirstShortlist.class);

  private static final int DEFAULT_SHORT_LIST_SIZE = 100;
  private static final int MAX_CANDIDATES = 100;
  private static final long MB = 1024*1024;
  private static final int FULL_SEARCH_LIST_SIZE = 200;
  private static final int MAX_TRIES = 1000000;

  public BreadthFirstShortlist(Binding binding) {
    super(binding);
  }

  public List<FL_PatternSearchResult> getShortlist(List<FL_EntityMatchDescriptor> entities1, List<String> exemplarIDs,
                                                        long max) {
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
   * Convert candidate graphs into pattern serach results.
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
