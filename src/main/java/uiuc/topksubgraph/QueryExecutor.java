/**
 *
 */
package uiuc.topksubgraph;

import mitll.xdata.binding.Binding;
import mitll.xdata.binding.TopKSubgraphShortlist;
import mitll.xdata.db.DBConnection;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.jgrapht.util.FibonacciHeap;
import org.jgrapht.util.FibonacciHeapNode;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Uses the sorted edge list and the MPW index indexes (in the metapath format) for query execution.
 *
 * @author Manish Gupta (gupta58@illinois.edu)
 *         University of Illinois at Urbana Champaign
 *         <p>
 *         Extended by @author Charlie Dagli (dagli@ll.mit.edu)
 *         MIT Lincoln Laboratory
 */
public class QueryExecutor {
  private static final Logger logger = Logger.getLogger(QueryExecutor.class);

  public static String datasetId;
  public static String baseDir;
  public static String graphFile;
  public static String graphFileBasename;
  public static String typesFile;
  public static String queryFile;
  public static String queryTypesFile;
  //public static String spathFile;
  public static String topologyFile;
  public static String spdFile;
  public static String resultDir;

  public static int topK;
  public static int k0;

  //no more statics after this...
  public Graph g;

  private Map<Integer, Integer> node2Type;
  private int totalTypes;

  private final List<Integer> types;
  private final Map<Integer, Set<Integer>> graphType2IDSet;
  //private int totalNodes;
  private int totalOrderingSize;
  private final Map<Integer, List<String>> ordering;
  private final Map<String, Integer> orderingType2Index;

  //private int[][] graphSign;
  private Map<Integer, List<Integer>> graphSign;

  private final Map<String, List<String>> sortedEdgeLists;
  private final Map<String, Map<Integer, List<Integer>>> node2EdgeListPointers;

  //  private double[][] spd;
  private Map<Integer, List<Float>> spd;
  private Graph query;
  //  private HashMap<Integer, Integer> queryNodeID2Type;
  private Map<Integer, Integer> queryNode2Type;
//  Set<Integer> queryNodeIds = new HashSet<>();

  //private int[][] querySign;
  private Map<Integer, List<Integer>> querySign;

  private Map<Integer, List<Integer>> candidates;

  private List<String> actualQueryEdges;

  private Map<String, Integer> queryEdgetoIndex;

  private Map<String, String> queryEdge2EdgeType;

  private Map<String, Integer> pointers;

  private FibonacciHeap<List<String>> heap;
  private Set<List<String>> heapSet;

  public QueryExecutor(Graph graph, String datasetId, String datasetResourceDir, Map<Integer, Integer> idToType) {
    this();
    QueryExecutor.datasetId = datasetId;
    QueryExecutor.baseDir = datasetResourceDir; //THIS LINE SHOULD CHANGE FOR JAR-ed VERSION

    this.g = graph;

    // QueryExecutor.spathFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".spath";
    QueryExecutor.topologyFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".topology";
    QueryExecutor.spdFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".spd";
    QueryExecutor.resultDir = "results";

    setNode2Type(idToType);
    computeTotalTypes();

    logger.info("total " + totalTypes);
    prepareInternals();
  }

  /**
   * Constructor
   *
   * @see mitll.xdata.binding.TopKSubgraphShortlist#TopKSubgraphShortlist(Binding)
   */
  public QueryExecutor() {

    datasetId = "";
    baseDir = "";
    graphFile = "";
    graphFileBasename = "";
    typesFile = "";
    queryFile = "";
    queryTypesFile = "";
    //spathFile = "";
    topologyFile = "";
    spdFile = "";
    resultDir = "";

    topK = 10;
    k0 = 2;

    g = new Graph();

    node2Type = new HashMap<>();
    totalTypes = 0;

    types = new ArrayList<>();
    graphType2IDSet = new HashMap<>();
    //totalNodes = 0;
    totalOrderingSize = 0;
    ordering = new HashMap<>();
    orderingType2Index = new HashMap<>();

    //graphSign initialized by loadGraphSignatures()

    sortedEdgeLists = new HashMap<>();
    node2EdgeListPointers = new HashMap<>();

    //spd initialized by loadSPDIndex()

    //query initialized by loadQuery()
    //  queryNodeID2Type = new HashMap<>();

    queryNode2Type = new HashMap<>();

    //querySign initalized by getQuerySignatures()

    candidates = new HashMap<>();

    actualQueryEdges = new ArrayList<>();

    queryEdgetoIndex = new HashMap<>();

    queryEdge2EdgeType = new HashMap<>();

    pointers = new HashMap<>();

    heap = new FibonacciHeap<>();
    heapSet = new HashSet<>();
  }

  public boolean testQuery(List<String> exemplarIDs, Graph graph, Map<Integer, Integer> idToType) {
        /*
     * Get all pairs of query nodes...
		 * (this is assuming ids are sortable by integer comparison, like in bitcoin)
		 */
    Set<Edge> queryEdges = getQueryEdges(exemplarIDs, graph);
    if (queryEdges.isEmpty()) return false;

    for (Edge qe : queryEdges) {
      logger.info("qe: " + qe);
    }

    boolean isClique = loadQuery(queryEdges, idToType);
    if (isClique) logger.warn("isClique: " + isClique);

    executeQuery(isClique);

    // Heap of results from uiuc.topksubgraph
    FibonacciHeap<List<String>> heap = getHeap();

    logger.info("Starting with: " + heap.size() + " matching sub-graphs...");

    // Loop-through resultant sub-graphs
    while (!heap.isEmpty()) {
      // Get matching sub-graph
      FibonacciHeapNode<List<String>> fhn = heap.removeMin();
      List<String> list = fhn.getData();

      // Sub-graph score
      double subgraphScore = fhn.getKey();

      logger.info("match score " + subgraphScore + " match " + getSubgraphNodes(list));
    }

    return true;
  }


  /**
   * Get nodes involved in subgraph from list of edges
   *
   * @param list
   * @paramx nodes
   * @see #testQuery(List, Graph, Map)
   */
  private Set<String> getSubgraphNodes(Collection<String> list) {
    Set<String> nodes = new HashSet<>();

    for (String aList : list) {
      // Get parts of edge
      String[] edgeSplit = aList.split("#");
      String src = edgeSplit[0];
      String dest = edgeSplit[1];
      /*
       * Track nodes
			 */
      if (!nodes.contains(src))
        nodes.add(src);
      if (!nodes.contains(dest))
        nodes.add(dest);
    }

    return nodes;
  }

  /**
   * @param exemplarIDs
   * @param graph
   * @return
   * @see #testQuery(List, Graph, Map)
   */
  private Set<Edge> getQueryEdges(List<String> exemplarIDs, Graph graph) {
    Set<Edge> queryEdges = new HashSet<>();
    Edge edg;
    int e1, e2;
    //String pair;

    logger.info("getQueryEdges ran on " + exemplarIDs);

    if (exemplarIDs.size() > 1) {
      for (int i = 0; i < exemplarIDs.size(); i++) {
        for (int j = i + 1; j < exemplarIDs.size(); j++) {
          String e1ID = exemplarIDs.get(i);
          e1 = Integer.parseInt(e1ID);
          String e2ID = exemplarIDs.get(j);
          e2 = Integer.parseInt(e2ID);

        //  logger.info("\tgetQueryEdges check : " + e1 + " " + e2);

          if (e1 <= e2) {
            //  pair = "(" + e1 + "," + e2 + ")";
            edg = new Edge(e1, e2, 1.0);  //put in here something to get weight if wanted...
          } else {
            //  pair = "(" + e2 + "," + e1 + ")";
            edg = new Edge(e2, e1, 1.0);  //put in here something to get weight if wanted...
          }

          // if (existsPair(graphTable, pairIDColumn, pair)) {
          boolean hasEdge = graph.getEdge(e1, e2) != null || graph.getEdge(e2, e1) != null;
          if (hasEdge) {
            queryEdges.add(edg);
          } else {
            logger.warn("getQueryEdges no edge between " + e1ID + " and " + e2ID + " : " + graph.getInLinksNodes());
          }
        }
      }
    }

    logger.info("getQueryEdges found " + queryEdges);

    return queryEdges;
  }

  public void executeQuery(boolean isClique) {
    /**
     * Get query signatures
     */
    getQuerySignatures(); //fills in querySign

    /**
     * NS Containment Check and Candidate Generation
     */
    long time1 = System.currentTimeMillis();

    int prunedCandidateFiltering = generateCandidates();
    //if (prunedCandidateFiltering < 0) {
    //	return;
    //}

    logger.info("Candidate Generation Time: " + (System.currentTimeMillis() - time1) + " millis.");


    /**
     * Populate all required HashMaps relating edges to edge-types
     */
    // compute edge types for all edges in query
    Set<String> queryEdgeTypes = computeQueryEdgeTypes();

    if (queryEdgeTypes.isEmpty()) {
      logger.error("huh? query edge types is empty???\n\n\n");
    }

    //compute queryEdgetoIndex
    computeQueryEdge2Index();

    //compute queryEdgeType2Edges
    Map<String, List<String>> queryEdgeType2Edges = computeQueryEdgeType2Edges();


    //Maintain pointers and topk heap
    computePointers(queryEdgeTypes, queryEdgeType2Edges);

    /**
     * The secret sauce... Execute the query...
     */
    executeQuery(queryEdgeType2Edges, isClique, prunedCandidateFiltering);

    long time2 = System.currentTimeMillis();
    logger.info("executeQuery Overall Time: " + (time2 - time1));


    //FibonacciHeap<ArrayList<String>> queryResults = executor.getHeap();
    //executor.printHeap();
    //executor.logHeap();

		/*
     *  Format results to influent API
		 */

    // subgraphs returned from executeQuery() are in the form of ordered edges.
    // this order aligns result edges to query edges. the issue is, there is no
    // mapping between query nodes roles (E0,E1,etc.) to result subgraph node roles.
    // this is what
  }


  public static void main(String[] args) throws Throwable {

    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();

    /**
     *  Initialze and read-in arguments
     */
    //init();
    baseDir = args[0];
    graphFile = args[1];
    typesFile = args[2];
    k0 = Integer.parseInt(args[3]);
    queryFile = args[4];
    queryTypesFile = args[5];
    //spathFile = args[6];
    topK = Integer.parseInt(args[7]);
    topologyFile = args[8];
    spdFile = args[9];
    resultDir = args[10];

    String pattern = Pattern.quote(System.getProperty("file.separator"));
    String[] splitGraphFile = graphFile.split(pattern);
    graphFileBasename = splitGraphFile[splitGraphFile.length - 1];

//		loadTypesFile();		//load-in types, and count how many there are
//		loadGraphNodesType();  	//compute ordering
//		loadGraphSignatures();  //topology
//		loadEdgeLists();  		//sorted edge lists
//		loadSPDIndex();   		//spd index
//
//			
//		//set system out to out-file...
//		System.setOut(new PrintStream(new File(baseDir+resultDir+"/QBSQueryExecutorV2.topK="+topK+"_K0="+k0+"_"+graphFileBasename.split("\\.")[0]+"_"+queryFile.split("/")[1])));
//		
//		/**
//		 * Read-in and setup query
//		 */
//		int isClique = loadQuery();
//		
//		getQuerySignatures(); //fills in querySign
//	
//		/**
//		 * NS Containment Check and Candidate Generation
//		 */
//		long time1=new Date().getTime();
//		
//		int prunedCandidateFiltering = generateCandidates();
//		if (prunedCandidateFiltering < 0) {
//			return;
//		}
//
//		long timeA = new Date().getTime();
//		logger.info("Candidate Generation Time: " + (timeA - time1));
//		
//		
//		/**
//		 * Populate all required HashMaps relating edges to edge-types 
//		 */
//		
//		// compute edge types for all edges in query
//		HashSet<String> queryEdgeTypes = computeQueryEdgeTypes();
//		
//		//compute queryEdgetoIndex
//		computeQueryEdge2Index();
//		
//		//compute queryEdgeType2Edges
//		HashMap<String, ArrayList<String>> queryEdgeType2Edges = computeQueryEdgeType2Edges();
//		
//		
//		//Maintain pointers and topk heap
//		computePointers(queryEdgeTypes, queryEdgeType2Edges);
//		
//
//		/**
//		 * The secret sauce... Execute the query...
//		 */
//		executeQuery(queryEdgeType2Edges, isClique, prunedCandidateFiltering);
//		
//		long time2=new Date().getTime();
//		logger.info("Overall Time: "+(time2-time1));
//		
//		printHeap();
  }


  /**
   * @see #executeQuery(boolean)
   * @see mitll.xdata.binding.TopKSubgraphShortlist#getShortlist(List, List, long)
   */
  public void executeQuery(Map<String, List<String>> queryEdgeType2Edges, boolean isClique, int prunedCandidateFiltering) {
    int prunedEdgeListsPartialCandidate = 0;
    int prunedMPWPartialCandidate = 0;
    int prunedEdgeListsSize1 = 0;
    int prunedMPWSize1 = 0;
    int prunedGlobal = 0;
    int pruningByMPWBetterThanThatByEdgeListsSize1 = 0;
    int pruningByMPWBetterThanThatByEdgeListsPartial = 0;
    int edgeProcessed = 0;

    while (true) {
      edgeProcessed++;
      if (edgeProcessed % 100 == 0)
        //logger.error("edgeProcessed: "+edgeProcessed);
        logger.debug("edgeProcessed: " + edgeProcessed);


      int end = 0;

      //if a pointer has reached the end of list, break
      for (String s : pointers.keySet()) {
        if (pointers.get(s) > sortedEdgeLists.get(s).size() - 1) {
          end = 1;
          break;
        }
      }
      if (end == 1)
        break;

      if (pointers.isEmpty()) {
        logger.error("huh? pointers is empty??\n\n\n");
      }
      //get edge with max score to be processed.
      String max = "";
      double maxScore = -1;
      for (String s : pointers.keySet()) {
        double val = Double.parseDouble(sortedEdgeLists.get(s).get(pointers.get(s)).split("#")[2]);
        if (val > maxScore) {
          maxScore = val;
          max = s;
        }
      }
//			logger.info("Edge-type with the largest score: "+max);
//			logger.info("Edge at the current index max is pointing to: "+sortedEdgeLists.get(max).get(pointers.get(max)));
//			logger.info("queryEdgetoIndex: "+ queryEdgetoIndex);
      //max is the type. How to get appropriate query edge/edges for this type?

			/*
       * Get query edges corresponding to edge-type of the largest unprocessed weighted-edge in sortedEdgeLists
			 */
      List<String> edgesOfMaxType = queryEdgeType2Edges.get(max);
      if (edgesOfMaxType == null) {
        logger.error("Something is wrong: '" + max + "'");
        edgesOfMaxType = queryEdgeType2Edges.get(max.split("#")[1] + "#" + max.split("#")[0]);
      }
      if (isClique) {
        List<String> edgesOfMaxType2 = new ArrayList<>();
        edgesOfMaxType2.add(edgesOfMaxType.get(0));
        edgesOfMaxType = edgesOfMaxType2;
      }
//			logger.info("edgesOfMaxType:"+edgesOfMaxType);

			
			/*
       * Loop-through query edges of edge-type "max"
			 */
      for (String queryEdge : edgesOfMaxType) {
//				logger.info("Here comes the queryEdge: "+queryEdge);

        // setup containers for candidate edges
        Set<List<String>> currCandidates = new HashSet<>();
        Set<List<String>> pcCurr = new HashSet<>();

        //which query edge are we working with
        int index = queryEdgetoIndex.get(queryEdge);

        // keep track of the query edges (and indices) we're currently considering...
        Set<String> consideredEdges = new HashSet<>();
        Set<Integer> consideredEdgeIndices = new HashSet<>();
        consideredEdges.add(queryEdge);
        consideredEdgeIndices.add(index);
//				logger.info("ConsideredEdges: "+consideredEdges);
//				logger.info("consideredEdgeIndices"+consideredEdgeIndices);

        // get candidate edge, "candidateEdge", flip around , if necessary, to match
        // edge-type "polarity" of queryEdge
        String candidateEdge = sortedEdgeLists.get(max).get(pointers.get(max));
        int q1 = queryNode2Type.get(Integer.parseInt(queryEdge.split("#")[0]));
        String[] edgeSplit = candidateEdge.split("#");
        int e1 = Integer.parseInt(edgeSplit[0]);

        if (!graphType2IDSet.get(q1).contains(e1))
          candidateEdge = edgeSplit[1] + "#" + edgeSplit[0] + "#" + edgeSplit[2];

        // place candidate-edge under consideration in it's hypothesized position
        // in a possible matching sub-graph ("list"). Add list to pcCurr
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < queryEdgetoIndex.size(); i++)
          list.add("");
        list.set(index, candidateEdge);
        pcCurr.add(list);

        // if homogeneous edge-type (i.candidateEdge. i#i or j#j) and query graph not a clique
        // track edge twice because itself and it's reflection are valid candidates
        if (max.split("#")[0].equals(max.split("#")[1]) && !isClique) {
          ArrayList<String> list1 = new ArrayList<>();
          for (int i = 0; i < queryEdgetoIndex.size(); i++)
            list1.add("");
          list1.set(index, edgeSplit[1] + "#" + edgeSplit[0] + "#" + edgeSplit[2]);
          pcCurr.add(list1);
        }

        //compute possible upper bound score of all non-considered query edges.
        double ubScoreOfNonConsideredEdges1 = 0.;
        for (String edge : queryEdgetoIndex.keySet()) {
          if (!consideredEdgeIndices.contains(queryEdgetoIndex.get(edge))) {
            String tmp = queryEdge2EdgeType.get(edge); //get edge-type
            if (!pointers.containsKey(tmp)) //order edge-type if necessary
              tmp = tmp.split("#")[1] + "#" + tmp.split("#")[0];
            // use highest score of that edge-type as upper-bound ("dumb" or "naive" upper bound)
            ubScoreOfNonConsideredEdges1 += Double.parseDouble(sortedEdgeLists.get(tmp).get(pointers.get(tmp)).split("#")[2]);
          }
        }
//				logger.info("pcCurr: "+pcCurr);

				/*
         * compute scores for current partial candidates
				 * add partial candidates to currCandidates if heap says they're worth considering 
				 */
        for (List<String> pc : pcCurr) {
          //compute actualScore of candidate
          double actualScore = 0.;
          for (int i : consideredEdgeIndices)
            actualScore += Double.parseDouble(pc.get(i).split("#")[2]);

          //compute personalized upper bound score of non-considered edges for this candidate
          double ubScoreOfNonConsideredEdges2 = Double.MAX_VALUE;
          ubScoreOfNonConsideredEdges2 = getUpperbound(consideredEdgeIndices, pc);

          //check to see which upper bound is tighter.
          double ubScoreOfNonConsideredEdges = 0;
          if (ubScoreOfNonConsideredEdges2 < ubScoreOfNonConsideredEdges1) {
            ubScoreOfNonConsideredEdges = ubScoreOfNonConsideredEdges2;
            //logger.error("Helps "+consideredEdgeIndices.size());
          } else {
            ubScoreOfNonConsideredEdges = ubScoreOfNonConsideredEdges1;
            //logger.error("No Help!"+consideredEdgeIndices.size());
          }
          //compute upper bound score of candidate
          double upperBoundScore = actualScore + ubScoreOfNonConsideredEdges;

          //check with heap and then add it to newCandidate
          if (heap.size() >= topK) {
            FibonacciHeapNode<List<String>> fhn = heap.min();
            //logger.info("Check:"+(actualScore+ubScoreOfNonConsideredEdges1)+"\t"+(actualScore+ubScoreOfNonConsideredEdges2)+"\t"+fhn.getKey());

            // add partial candidate pc if there's room in the heap for it
            if (upperBoundScore > fhn.getKey())
              currCandidates.add(pc);
            else {
              //gather some statistics about which upper-bounding strategy is better
              if (ubScoreOfNonConsideredEdges2 > ubScoreOfNonConsideredEdges1)
                prunedEdgeListsSize1++;
              else
                prunedMPWSize1++;
              if (ubScoreOfNonConsideredEdges2 < ubScoreOfNonConsideredEdges1 && ubScoreOfNonConsideredEdges1 + actualScore > fhn.getKey())
                pruningByMPWBetterThanThatByEdgeListsSize1++;
              //logger.info("TopK pruned!"+"\t"+(actualScore+ubScoreOfNonConsideredEdges1)+"\t"+(actualScore+ubScoreOfNonConsideredEdges2)+"\t"+fhn.getKey());
            }
          } else
            // if heap is not yet full, pc gets added by default
            currCandidates.add(pc);
        }

				/*
         *
				 */
        if (currCandidates.size() == 0)
          continue;
        while (consideredEdges.size() != queryEdgetoIndex.size()) {
          Set<List<String>> newCandidates = new HashSet<>();
          //get set of edges in Q that connect to consideredEdges but not in consideredEdges
          Set<String> verticesCovered = new HashSet<>();
          for (String s : consideredEdges) {
            verticesCovered.add(s.split("#")[0]);
            verticesCovered.add(s.split("#")[1]);
          }
          List<String> nextEdgeCandidates = new ArrayList<>();
          for (String s : actualQueryEdges) {
            String v1 = s.split("#")[0];
            String v2 = s.split("#")[1];
            if ((verticesCovered.contains(v1) && !consideredEdges.contains(s)) || (verticesCovered.contains(v2) && !consideredEdges.contains(s)))
              nextEdgeCandidates.add(s);
          }
          int rand = 0;//(int) (Math.random()*nextEdgeCandidates.size());
          if (nextEdgeCandidates.size() == 0) {
            logger.error("Cannot process this query");
            return;
          }
          String nextEdge = nextEdgeCandidates.get(rand);
          int n1 = Integer.parseInt(nextEdge.split("#")[0]);
          int n2 = Integer.parseInt(nextEdge.split("#")[1]);
          //find the edge which can tell us about the actual vertex/vertices instantiations
          //first find if only one or both vertices are already in consideredEdges.
          e1 = -1;
          int e2 = -1;
          int pos1 = -1;
          int pos2 = -1;
          for (String s : consideredEdges) {
            int v1 = Integer.parseInt(s.split("#")[0]);
            int v2 = Integer.parseInt(s.split("#")[1]);
            if (v1 == n1 || v2 == n1) {
              e1 = queryEdgetoIndex.get(s);
              if (v1 == n1)
                pos1 = 1;
              else
                pos1 = 2;
            }
            if (v1 == n2 || v2 == n2) {
              e2 = queryEdgetoIndex.get(s);
              if (v1 == n2)
                pos2 = 1;
              else
                pos2 = 2;
            }
          }
          int t1 = queryNode2Type.get(n1);
          int t2 = queryNode2Type.get(n2);
          String nextEdgeType = t1 + "#" + t2;//this is T_{candidateEdge'}
          if (t1 > t2)
            nextEdgeType = t2 + "#" + t1;
          int edgeIndex = queryEdgetoIndex.get(nextEdge);
          consideredEdges.add(nextEdge);
          consideredEdgeIndices.add(edgeIndex);
//					logger.info("currCandidates: "+currCandidates);
          for (List<String> c : currCandidates) {
            //Find matching edges from useful edge list of T_{candidateEdge'} and extend candidate c to candidate c'.
            int node1 = -1;
            int node2 = -1;
            if (e1 != -1)
              node1 = Integer.parseInt(c.get(e1).split("#")[pos1 - 1]);
            if (e2 != -1)
              node2 = Integer.parseInt(c.get(e2).split("#")[pos2 - 1]);
            List<Integer> edgeIDs1 = new ArrayList<>();
            List<Integer> edgeIDs2 = new ArrayList<>();
            if (node1 != -1)
              edgeIDs1 = node2EdgeListPointers.get(nextEdgeType).get(node1);
            if (node2 != -1)
              edgeIDs2 = node2EdgeListPointers.get(nextEdgeType).get(node2);
            if (node1 == node2)
              continue;
            HashSet<ArrayList<String>> potentialCandidates = new HashSet<>();
            if (node1 != -1 && node2 != -1) {
              //compute the intersection
              ArrayList<Integer> intersection = new ArrayList<>();
              if (edgeIDs1 != null && edgeIDs2 != null) {
                for (int k : edgeIDs1)
                  if (edgeIDs2.contains(k))
                    intersection.add(k);
              }
              for (int k : intersection) {
                //create candidate
                ArrayList<String> newCandi = new ArrayList<>();
                int flag = 0;
                String ee = node1 + "#" + node2 + "#" + sortedEdgeLists.get(nextEdgeType).get(k).split("#")[2];//;
                String ree = node2 + "#" + node1 + "#" + sortedEdgeLists.get(nextEdgeType).get(k).split("#")[2];//;
                for (String s : c) {
                  if (s.equals(ee) || s.equals(ree)) {
                    flag = 1;//do not add an old edge in the candidate
                    break;
                  }
                  newCandi.add(s);
                }
                if (flag == 1)
                  continue;
                newCandi.set(edgeIndex, ee);
                potentialCandidates.add(newCandi);
              }
            } else if (node1 == -1 && node2 != -1) {
              if (edgeIDs2 != null) {
                for (int k : edgeIDs2) {
                  ArrayList<String> newCandi = new ArrayList<>();
                  int flag = 0;
                  String ee = sortedEdgeLists.get(nextEdgeType).get(k);
                  if (Integer.parseInt(ee.split("#")[1]) != node2)
                    ee = ee.split("#")[1] + "#" + ee.split("#")[0] + "#" + ee.split("#")[2];
                  String ree = ee.split("#")[1] + "#" + ee.split("#")[0] + "#" + ee.split("#")[2];
                  for (String s : c) {
                    if (s.equals(ee) || s.equals(ree)) {
                      flag = 1;//do not add an old edge in the candidate
                      break;
                    }
                    newCandi.add(s);
                  }
                  if (flag == 1)
                    continue;
                  newCandi.set(edgeIndex, ee);
                  potentialCandidates.add(newCandi);
                }
              }
            } else if (node1 != -1 && node2 == -1) {
              if (edgeIDs1 != null) {
                for (int k : edgeIDs1) {
                  ArrayList<String> newCandi = new ArrayList<>();
                  int flag = 0;
                  String ee = sortedEdgeLists.get(nextEdgeType).get(k);
                  if (Integer.parseInt(ee.split("#")[0]) != node1)
                    ee = ee.split("#")[1] + "#" + ee.split("#")[0] + "#" + ee.split("#")[2];
                  String ree = ee.split("#")[1] + "#" + ee.split("#")[0] + "#" + ee.split("#")[2];
                  for (String s : c) {
                    if (s.equals(ee) || s.equals(ree)) {
                      flag = 1;//do not add an old edge in the candidate
                      break;
                    }
                    newCandi.add(s);
                  }
                  if (flag == 1)
                    continue;
                  newCandi.set(edgeIndex, ee);
                  potentialCandidates.add(newCandi);
                }
              }
            }
            //compute possible upper bound score of all non-considered edges.
            ubScoreOfNonConsideredEdges1 = 0.;
            for (String edge : queryEdgetoIndex.keySet()) {
              if (!consideredEdgeIndices.contains(queryEdgetoIndex.get(edge))) {
                String tmp = queryEdge2EdgeType.get(edge);
                if (!pointers.containsKey(tmp))
                  tmp = tmp.split("#")[1] + "#" + tmp.split("#")[0];
                ubScoreOfNonConsideredEdges1 += Double.parseDouble(sortedEdgeLists.get(tmp).get(pointers.get(tmp)).split("#")[2]);
              }
            }
            for (ArrayList<String> pc : potentialCandidates) {
              //compute actualScore of candidate
              double actualScore = 0.;
              for (int i : consideredEdgeIndices)
                actualScore += Double.parseDouble(pc.get(i).split("#")[2]);
              //compute personalized upper bound score of non-considered edges for this candidate
              double ubScoreOfNonConsideredEdges2 = Double.MAX_VALUE;
//							ubScoreOfNonConsideredEdges2=getUpperbound(consideredEdgeIndices, pc);
              //check which upper bound is tight.
              double ubScoreOfNonConsideredEdges = 0;
              if (ubScoreOfNonConsideredEdges2 < ubScoreOfNonConsideredEdges1) {
                ubScoreOfNonConsideredEdges = ubScoreOfNonConsideredEdges2;
//								logger.error("Helps "+consideredEdgeIndices.size());
              } else {
                ubScoreOfNonConsideredEdges = ubScoreOfNonConsideredEdges1;
//								logger.error("No Help!"+consideredEdgeIndices.size());
              }
              //compute upper bound score of candidate
              double upperBoundScore = actualScore + ubScoreOfNonConsideredEdges;
              //check with heap and then add it to newCandidate
              if (heap.size() >= topK) {
                FibonacciHeapNode<List<String>> fhn = heap.min();
                if (upperBoundScore > fhn.getKey()) {
                  newCandidates.add(pc);
                } else {
                  //candidate is pruned
                  if (ubScoreOfNonConsideredEdges2 > ubScoreOfNonConsideredEdges1)
                    prunedEdgeListsPartialCandidate++;
                  else
                    prunedMPWPartialCandidate++;
                  if (ubScoreOfNonConsideredEdges2 < ubScoreOfNonConsideredEdges1 && ubScoreOfNonConsideredEdges1 + actualScore > fhn.getKey())
                    pruningByMPWBetterThanThatByEdgeListsPartial++;
                }
              } else
                newCandidates.add(pc);
            }
          }
          currCandidates = newCandidates;
        }

        //Update Heap using CurrCandidates
        for (List<String> c : currCandidates) {
          double actualScore = 0.;
          for (int i : consideredEdgeIndices)
            actualScore += Double.parseDouble(c.get(i).split("#")[2]);
          if (heapSet.contains(c))
            continue;
          if (heap.size() >= topK) {
            FibonacciHeapNode<List<String>> fhn = heap.min();
            if (actualScore > fhn.getKey()) {
              FibonacciHeapNode<List<String>> fhn2 = heap.removeMin();
              heapSet.remove(fhn2.getData());
              FibonacciHeapNode<List<String>> fhn1 = new FibonacciHeapNode<>(c, actualScore);
              heap.insert(fhn1, fhn1.getKey());
              heapSet.add(fhn1.getData());
            }
          } else {
            FibonacciHeapNode<List<String>> fhn = new FibonacciHeapNode<>(c, actualScore);
            heap.insert(fhn, fhn.getKey());
            heapSet.add(fhn.getData());
          }
        }
      }


      //Move pointer to next position in useful edge list of e.
      List<String> list = sortedEdgeLists.get(max);
      List<String> arr1 = queryEdgeType2Edges.get(max);
      int old = pointers.get(max);
      for (int c = pointers.get(max) + 1; c < list.size(); c++) {
        String l = list.get(c);
        String tokens[] = l.split("#");
        int n1 = Integer.parseInt(tokens[0]);
        int n2 = Integer.parseInt(tokens[1]);
        int flag = 0;
        for (String ee : arr1) {
          int v1 = Integer.parseInt(ee.split("#")[0]);
          int v2 = Integer.parseInt(ee.split("#")[1]);
          if ((candidates.get(v1).contains(n1) && candidates.get(v2).contains(n2)) || (candidates.get(v1).contains(n2) && candidates.get(v2).contains(n1))) {
            flag = 1;
            break;
          }
        }
        if (flag == 1) {
          pointers.put(max, c);
          break;
        }
      }

      if (old == pointers.get(max))
        pointers.put(max, list.size());
      if (heap.size() == topK) {
        //Compute UpperBoundScore for any new candidate (using scores at pointer positions).
        double maxUpperBound = 0.;
        for (String actualQueryEdge : actualQueryEdges) {
          String type = queryEdge2EdgeType.get(actualQueryEdge);
          if (!sortedEdgeLists.containsKey(type))
            type = type.split("#")[1] + "#" + type.split("#")[0];
          if (sortedEdgeLists.get(type).size() > pointers.get(type))
            maxUpperBound += Double.parseDouble(sortedEdgeLists.get(type).get(pointers.get(type)).split("#")[2]);
        }
//				for(String edgeType:usefulSortedEdgeLists.keySet())
//				{
//					String reversedEdgeType=edgeType.split("#")[1]+"#"+edgeType.split("#")[0];
//					if(queryEdgeType2Edges.containsKey(edgeType))
//						maxUpperBound+=queryEdgeType2Edges.get(edgeType).size()*(Double.parseDouble(sortedEdgeLists.get(edgeType).get(pointers.get(edgeType)).split("#")[2]));
//					else
//						maxUpperBound+=queryEdgeType2Edges.get(reversedEdgeType).size()*(Double.parseDouble(sortedEdgeLists.get(edgeType).get(pointers.get(edgeType)).split("#")[2]));
//				}
        //if UpperBoundScore < Score(minElementInHeap) then
        //{
        //topK Quit
        //}
        FibonacciHeapNode<List<String>> fhn = heap.min();
        if (maxUpperBound < fhn.getKey()) {
          //printHeap();
          logger.info("Top-K Quit");
          prunedGlobal = 1;
          logger.info("Pruning Stats: " + prunedCandidateFiltering + "\t" + prunedEdgeListsPartialCandidate + "\t" + prunedGlobal + "\t" + prunedMPWPartialCandidate + "\t" + prunedEdgeListsSize1 + "\t" + prunedMPWSize1 + "\t" + pruningByMPWBetterThanThatByEdgeListsSize1 + "\t" + pruningByMPWBetterThanThatByEdgeListsPartial);
          return;
        }
      }
    }

    //printHeap();

    logger.error("edgeProcessed: " + edgeProcessed);
    logger.info("Pruning Stats: " + prunedCandidateFiltering + "\t" + prunedEdgeListsPartialCandidate + "\t" + prunedGlobal + "\t" + prunedMPWPartialCandidate + "\t" + prunedEdgeListsSize1 + "\t" + prunedMPWSize1 + "\t" + pruningByMPWBetterThanThatByEdgeListsSize1 + "\t" + pruningByMPWBetterThanThatByEdgeListsPartial);

  }

  /**
   * @param queryEdgeTypes
   * @param queryEdgeType2Edges
   * @throws NumberFormatException
   * @see #executeQuery(boolean)
   */
  public void computePointers(Collection<String> queryEdgeTypes,
                              Map<String, List<String>> queryEdgeType2Edges)
      throws NumberFormatException {
    pointers = new HashMap<>();
    for (String edgeType : queryEdgeTypes) {
      int t1 = Integer.parseInt(edgeType.split("#")[0]);
      int t2 = Integer.parseInt(edgeType.split("#")[1]);
      String orderedEdgeType = t1 + "#" + t2;
      if (t1 > t2)
        orderedEdgeType = t2 + "#" + t1;
      List<String> list = sortedEdgeLists.get(orderedEdgeType);
      List<String> arr1 = queryEdgeType2Edges.get(orderedEdgeType);
      for (int c = 0; c < list.size(); c++) {
        String l = list.get(c);
        String tokens[] = l.split("#");
        int n1 = Integer.parseInt(tokens[0]);
        int n2 = Integer.parseInt(tokens[1]);
        int flag = 0;
        for (String ee : arr1) {
          int v1 = Integer.parseInt(ee.split("#")[0]);
          int v2 = Integer.parseInt(ee.split("#")[1]);
          if ((candidates.get(v1).contains(n1) && candidates.get(v2).contains(n2)) || (!orderedEdgeType.equals(edgeType) && candidates.get(v1).contains(n2) && candidates.get(v2).contains(n1))) {
            flag = 1;
            break;
          }
        }
        if (flag == 1) {
          pointers.put(orderedEdgeType, c);
          break;
        }
      }
      if (!pointers.containsKey(orderedEdgeType))
        pointers.put(orderedEdgeType, list.size());
    }
  }

  /**
   * TODO : why store type list in an array?
   *
   * @return
   * @throws NumberFormatException
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#executeQuery(String, DBConnection)
   */
  public Map<String, List<String>> computeQueryEdgeType2Edges()
      throws NumberFormatException {
    Map<String, List<String>> queryEdgeType2Edges = new HashMap<>();
    //queryEdge2EdgeType = new HashMap<String, String>(); //this has already been initialized twice...
    //for(String s:queryEdgeTypes)
    //	queryEdgeType2Edges.put(s, new ArrayList<String>());
    for (String qe : actualQueryEdges) {
      int n1 = Integer.parseInt(qe.split("#")[0]);
      int n2 = Integer.parseInt(qe.split("#")[1]);
      int t1 = queryNode2Type.get(n1);
      int t2 = queryNode2Type.get(n2);
      String type = t1 + "#" + t2;
      if (t1 > t2)
        type = t2 + "#" + t1;
      List<String> tmp = new ArrayList<>();
      if (queryEdgeType2Edges.containsKey(type))
        tmp = queryEdgeType2Edges.get(type);
      tmp.add(qe);
      queryEdgeType2Edges.put(type, tmp);
      //queryEdgeType2Edges.get(type).add(qe);
      queryEdge2EdgeType.put(qe, type);
    }
    return queryEdgeType2Edges;
  }

  /**
   *
   */
  public void computeQueryEdge2Index() {
    queryEdgetoIndex = new HashMap<>();
    for (int i = 0; i < actualQueryEdges.size(); i++)
      queryEdgetoIndex.put(actualQueryEdges.get(i), i);
  }

  /**
   * TODO : I thought that the edge ids were in raw space???
   *
   * @return Edge Types for all edges in query
   * @see QueryExecutor#executeQuery(boolean)
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#executeQuery(String, DBConnection)
   * @see
   */
  public Set<String> computeQueryEdgeTypes() {
    Set<Edge> queryEdgeSet = query.getEdges();
    //actualQueryEdges= new ArrayList<String>();
    Set<String> queryEdgeTypes = new HashSet<>();

//    if (queryNodeID2Type.isEmpty()) {
//      logger.error("computeQueryEdgeTypes : huh? queryNodeID2Type is empty ");
//    }
    for (Edge e : queryEdgeSet) {
      int src = e.getSrc();
      //int n1 = query.getRawID(src);
      int dst = e.getDst();
      //  int n2 = query.getRawID(dst);
      // if (n1 <= n2) {
      if (src <= dst) {
        //actualQueryEdges.add(n1 + "#" + n2);
        actualQueryEdges.add(src + "#" + dst);
//        int t1 = queryNodeID2Type.get(src);
//        int t2 = queryNodeID2Type.get(dst);

        int t1 = queryNode2Type.get(src);
        int t2 = queryNode2Type.get(dst);
        //if(t1<t2)
        queryEdgeTypes.add(t1 + "#" + t2);
      }
    }
    return queryEdgeTypes;
  }

  /**
   * @return prunedCandidateFiltering:
   */
  public int generateCandidates() {
    int prunedCandidateFiltering = 0;

    //  for (int i = 0; i < query.getNumNodes(); i++) {
    for (Integer nodeID : query.getRawIDs()) {
      //   Integer nodeType = queryNodeID2Type.get(i);
      Integer nodeType = queryNode2Type.get(nodeID);
      Set<Integer> c1 = graphType2IDSet.get(nodeType);

      if (c1 == null) {
        logger.error("generateCandidates Graph has no nodes of type " + nodeType);
        return -1;
      } else {
//        logger.debug("generateCandidates for " + nodeID + " got " + c1);
      }

      // logger.info("Old Size: "+c1.size());
      List<Integer> c2 = new ArrayList<>();
      for (int c : c1) {
        int kstar = NSContained(nodeID, c);
        // logger.info(kstar+" kstar: "+c);
        if (kstar != -1)
          c2.add(c);
      }

      if (c2.size() == 0) {
        logger.error("Graph has no candidate nodes of type " + nodeType);
        return -1;
      }

      logger.info("New Size: " + c2.size());
      //  Integer rawID = nodeID;//query.getRawID(i);
      candidates.put(nodeID, c2);
      prunedCandidateFiltering += (c1.size() - c2.size());
    }
    logger.info("generateCandidates " + prunedCandidateFiltering);
    return prunedCandidateFiltering;
  }

  /**
   * Load-in the query graph
   *
   * @return isClique: whether the query graph is a clique
   * @throws Throwable
   * @throws FileNotFoundException
   * @throws IOException
   * @throws NumberFormatException
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#executeQuery(String, DBConnection)
   * @see IngestAndQuery#executeQuery()
   * @deprecated why are we readying the query graph from a file???
   */
  public boolean loadQuery() throws Throwable {
    //initialize (or re-initialize if loading new query)
    query = new MutableGraph(new File(baseDir, queryFile));
    // queryNodeID2Type = new HashMap<>();
    queryNode2Type = new HashMap<>();

    boolean isClique = getIsClique();

    //read query types
    BufferedReader in = new BufferedReader(new FileReader(new File(baseDir, queryTypesFile)));
    String str = "";
    while ((str = in.readLine()) != null) {
      String tokens[] = str.split("\\t");
//      queryNodeID2Type.put(query.getInternalID(Integer.parseInt(tokens[0])), Integer.parseInt(tokens[1]));
      queryNode2Type.put(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
    }
    in.close();
    return isClique;
  }

  /**
   * @param queryEdges
   * @param idToType
   * @return
   * @see #testQuery(List, Graph, Map)
   */
  private boolean loadQuery(Set<Edge> queryEdges, Map<Integer, Integer> idToType) {
    if (queryEdges.isEmpty()) {
      logger.error("loadQuery huh? no query edges???");
      return false;
    }
    loadGraph(queryEdges);

		/*
		 * Read query types
		 */
    Collection<Integer> queryNodes = query.getRawIDs();

    if (queryNodes.isEmpty()) {
      logger.error("loadQuery huh? no query nodes in " + query);
    } else {
      logger.debug("loadQuery query nodes " + queryNodes);
    }

    for (int node : queryNodes) {
      Integer type = idToType.get(node);
      if (type == null) {
        logger.error("huh? no type for " + node);
      }
      //  queryNodeID2Type.put(query.getInternalID(node), type);
      queryNode2Type.put(node, type);
    }

    logger.info("loadQuery query maps " + queryNode2Type);

    return getIsClique();
  }

  /**
   * Load-in the query graph from HashSet of edges
   *
   * @param queryEdges
   * @param connection
   * @param tableName
   * @param uidColumn
   * @param typeColumn
   * @return true if graph is a clique
   * @see mitll.xdata.binding.TopKSubgraphShortlist#getShortlist(List, List, long)
   */
  public boolean loadQuery(Collection<Edge> queryEdges, Connection connection, String tableName,
                           String uidColumn, String typeColumn) {
    loadGraph(queryEdges);

		/*
		 * Read query types
		 */
    Collection<Integer> queryNodes = query.getRawIDs();

    loadQueryTypes(connection, tableName, uidColumn, typeColumn, queryNodes);

    return getIsClique();
  }

  private void loadGraph(Collection<Edge> queryEdges) {
    //initialize (or re-initialize if loading new query)
    initQuery();

    //read query graph
    query = new MutableGraph(queryEdges);
  }

  private boolean getIsClique() {
    boolean isClique = false;
    if (query.getNumEdges() == (query.getNumNodes() * (query.getNumNodes() - 1) / 2)) {
      logger.error("Query is Clique");
      isClique = true;
    }
    return isClique;
  }

  private void loadQueryTypes(Connection connection, String tableName, String uidColumn, String typeColumn,
                              Collection<Integer> queryNodes) {
    for (int node : queryNodes) {
      String sqlQuery = "select " + typeColumn + " from " + tableName + " where " + uidColumn + "=" + node + ";";

      int type = 0;
      try {
        PreparedStatement queryStatement = connection.prepareStatement(sqlQuery);
        ResultSet rs = queryStatement.executeQuery();
        rs.next();

        type = rs.getInt(typeColumn);

        rs.close();
        queryStatement.close();
      } catch (SQLException e) {
        logger.info("Got e: " + e);
      }

//      queryNodeID2Type.put(query.getInternalID(node), type);
      queryNode2Type.put(node, type);
    }
  }


  /**
   * Re-initialize all variables changed by Query Execution
   */
  private void initQuery() {
    query = new Graph();
    //  queryNodeID2Type = new HashMap<>();
    queryNode2Type = new HashMap<>();
    //querySign always gets re-initialized in getQuerySignatures()
    candidates = new HashMap<>();
    actualQueryEdges = new ArrayList<>();
    queryEdgetoIndex = new HashMap<>();
    queryEdge2EdgeType = new HashMap<>();
    pointers = new HashMap<>();
    heap = new FibonacciHeap<>();
    heapSet = new HashSet<>();
  }


  /**
   *
   */
/*  public void init() {
    ordering = new HashMap<Integer, ArrayList<String>>();
    orderingType2Index = new HashMap<String, Integer>();
    topK = 10;
    totalTypes = 0;
    heap = new FibonacciHeap<ArrayList<String>>();
    heapSet = new HashSet<ArrayList<String>>();
    candidates = new HashMap<Integer, ArrayList<Integer>>();
    queryNodeID2Type = new HashMap<Integer, Integer>();
    queryNode2Type = new HashMap<Integer, Integer>();
    graphType2IDSet = new HashMap<Integer, HashSet<Integer>>();
    types = new ArrayList<Integer>();
    sortedEdgeLists = new HashMap<String, ArrayList<String>>();
    node2EdgeListPointers = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
    actualQueryEdges = new ArrayList<String>();
    pointers = new HashMap<String, Integer>();
    queryEdgetoIndex = new HashMap<String, Integer>();
    queryEdge2EdgeType = new HashMap<String, String>();
  }*/

  /**
   *
   */
  public void printHeap() {
    logger.info("============================================================================");
    while (!heap.isEmpty()) {
      FibonacciHeapNode<List<String>> fhn = heap.removeMin();
      List<String> list = fhn.getData();
      for (String aList : list) System.out.print(aList + "\t");
      System.out.print(fhn.getKey());
      System.out.println();
    }
    logger.info("============================================================================");
//		System.exit(0);
  }

/*  public void logHeap() {
    logger.info("============================================================================");
    while (!heap.isEmpty()) {
      FibonacciHeapNode<List<String>> fhn = heap.removeMin();
      ArrayList<String> list = fhn.getData();
      String line = "";
      for (int i = 0; i < list.size(); i++) {
        String[] edgeSplit = list.get(i).split("#");
        String src = edgeSplit[0];
        String dest = edgeSplit[1];
        double weight = Double.parseDouble(edgeSplit[2]);

        line = "src: " + src + " dest: " + dest + " weight: " + weight + "\t";

      }
      logger.info(line);
    }
    logger.info("============================================================================");
  }*/

  /**
   * @throws Throwable
   */
  public void loadSPDIndex() throws Throwable {
    //  spd = new double[totalNodes][totalOrderingSize];
    spd = new HashMap<>();
    BufferedReader in = new BufferedReader(new FileReader(new File(baseDir, spdFile)));
    String str = "";
    while ((str = in.readLine()) != null) {
      String tokens[] = str.split("\\s+");
      int node = Integer.parseInt(tokens[0]);

      List<Float> maxWeightForTypeCombo = new ArrayList<>();
      spd.put(node, maxWeightForTypeCombo);

      String toks[] = tokens[1].split(";");
      for (String tok : toks) {
        //spd[node-1][t2]=Double.parseDouble(toks[t2]);
        //spd[node][t2] = Double.parseDouble(toks[t2]);
        maxWeightForTypeCombo.add(Float.parseFloat(tok));
      }
    }
    in.close();
  }

  /**
   * TODO : don't store edge lists as arrays
   *
   * @throws Throwable
   * @see #prepareInternals()
   */
  public void loadEdgeLists() throws Throwable {
    for (int i = 1; i <= totalTypes; i++) {
      for (int j = i; j <= totalTypes; j++) {
        //BufferedReader in = new BufferedReader(new FileReader(new File(baseDir+"indices/"+graphFileBasename.split("\\.txt")[0]+"_"+i+"#"+j+".list")));
        File edgeListFile = new File(baseDir + datasetId + "_" + i + "#" + j + ".list");

//        logger.info("reading " + edgeListFile.getAbsolutePath() + " exists " + edgeListFile.exists() + " baseDir " + baseDir);

        BufferedReader in = new BufferedReader(new FileReader(edgeListFile));
        String str = "";
        List<String> list = new ArrayList<>();
        while ((str = in.readLine()) != null)
          list.add(str);
        in.close();
        sortedEdgeLists.put(i + "#" + j, list);
      }
    }
    //also create node pointers to lists.
    for (String s : sortedEdgeLists.keySet()) {
      Map<Integer, List<Integer>> map = new HashMap<>();//vertex to indices
      List<String> list = sortedEdgeLists.get(s);
      for (int i = 0; i < list.size(); i++) {
        String e = list.get(i);
        String[] split = e.split("#");
        int v1 = Integer.parseInt(split[0]);
        int v2 = Integer.parseInt(split[1]);

        // TODO : rationalize this stuff

        List<Integer> arr1 = new ArrayList<>();
        if (map.containsKey(v1))
          arr1 = map.get(v1);
        arr1.add(i);
        map.put(v1, arr1);
        arr1 = new ArrayList<>();
        if (map.containsKey(v2))
          arr1 = map.get(v2);
        arr1.add(i);
        map.put(v2, arr1);
      }
      node2EdgeListPointers.put(s, map);
    }
  }

  /**
   * @param v
   * @param u
   * @return
   * @see #generateCandidates()
   */
  private int NSContained(int v, int u) {
    int count[] = new int[types.size()];
    int kstar = k0;
    int oc = 0;
//    logger.debug("NSContained " + g);

    for (int k1 = 1; k1 <= ordering.size(); k1++) {
      for (int k2 = 1; k2 <= ordering.get(k1).size(); k2++) {
        int chunk = ordering.get(k1).size() / totalTypes;
        int localType = (k2 - 1) / chunk;
        //count[localType]+=graphSign[u-1][oc];
        Integer rawID = u;//g.getInternalID(u);
        //  Integer rawID = u;
        //   Integer rawID = g.getRawID(u);
        //if (rawID == null) {
        //  logger.warn("NSContained no node for '" + u + "' in " + g.getRawIDs() );
        //  }
        //else {
        // count[localType] += graphSign[rawID][oc];
        count[localType] += graphSign.get(rawID).get(oc);//[oc];
        //     double v1 = querySign[v][oc];
        double v1 = querySign.get(v).get(oc);
        if (v1 > count[localType])
          return -1;
        count[localType] -= v1;
        oc++;
        // }
      }
    }
    return kstar;
  }

  public void loadGraphSignatures() throws Throwable {
    //  graphSign = new int[totalNodes][totalOrderingSize];
    graphSign = new HashMap<>();
    File file = new File(baseDir, topologyFile);

    logger.info("reading topology from " + file.getAbsolutePath() + " base " + baseDir + " top " + topologyFile);

    BufferedReader in = new BufferedReader(new FileReader(file));
    String str = "";
    while ((str = in.readLine()) != null) {
      String tokens[] = str.split("\\s+");
      int node = Integer.parseInt(tokens[0]);

      List<Integer> topologyForNode = new ArrayList<>();
      graphSign.put(node, topologyForNode);

      String toks[] = tokens[1].split(";");
      for (String tok : toks) {
//				graphSign[node-1][t2]=Integer.parseInt(toks[t2]);
        //  graphSign[node][t2] = Integer.parseInt(toks[t2]);
        topologyForNode.add(Integer.parseInt(tok));
      }
    }
    in.close();
  }

  /**
   * @see TopKSubgraphShortlist#loadTypesAndIndices()
   */
  public void prepareInternals() {
    logger.info("Loading in indices...");

    try {
      loadGraphNodesType();    //compute ordering
      loadGraphSignatures();    //topology
      loadEdgeLists();      //sorted edge lists
      loadSPDIndex();      //spd index
    } catch (Throwable e) {
      logger.error("Got " + e, e);
    }
  }

  /**
   * @throws Throwable
   * @see #prepareInternals()
   * @see TopKSubgraphShortlist#loadTypesAndIndices()
   */
  public void loadGraphNodesType() {
    for (int t = 1; t <= totalTypes; t++) {
      types.add(t);
      graphType2IDSet.put(t, new HashSet<>());
    }

    logger.info("loadGraphNodesType graphType2IDSet " + graphType2IDSet.size());

    for (int n : node2Type.keySet()) {
      Integer key = node2Type.get(n);
//      logger.debug("\t " + n + "-> " + key);
      graphType2IDSet.get(key).add(n);
    }

    logger.info("loadGraphNodesType graphType2IDSet " + graphType2IDSet.size());// + " : " + graphType2IDSet.size());

    //totalNodes = node2Type.size();
    //fix the ordering
    //generate the map from type string to index.
    totalOrderingSize = 0;
    for (int d = 1; d <= k0; d++)
      ordering.put(d, new ArrayList<>());
    for (int i = 1; i <= totalTypes; i++) {
      ordering.get(1).add(i + "");
      orderingType2Index.put(i + "", totalOrderingSize);
      totalOrderingSize++;
    }

    logger.info("ordering " + ordering.keySet());
    for (int d = 2; d <= k0; d++) {
      for (int i = 1; i <= totalTypes; i++) {
        for (String s : ordering.get(d - 1)) {
          if (s.length() == d - 1) {
            String e = s + i;
            ordering.get(d).add(e);
            orderingType2Index.put(e, totalOrderingSize);
            totalOrderingSize++;
          }
        }
      }
    }
  }

  /**
   * @see #executeQuery(boolean)
   */
  public void getQuerySignatures() {
    //  querySign = new int[query.getNumNodes()][totalOrderingSize];
    querySign = new HashMap<>();
    //  for (int i = 0; i < query.getNumNodes(); i++) {
    for (Integer nodeID : query.getRawIDs()) {
      Set<Path> set = getPaths(nodeID, new HashSet<>());
      Map<String, List<Integer>> topo = new HashMap<>();
      querySign.put(nodeID, new ArrayList<>());

      for (Path p : set) {
        String types = "";

        // TODO : better here too
        for (int j = 1; j < p.nodes.size(); j++) {
          //  types += queryNodeID2Type.get(p.nodes.get(j));
          types += queryNode2Type.get(p.nodes.get(j));
        }

        // TODO : rationalize this
        List<Integer> l = new ArrayList<>();
        if (topo.containsKey(types))
          l = topo.get(types);
        int lastNode = p.nodes.get(p.nodes.size() - 1);
        if (!l.contains(lastNode))
          l.add(lastNode);
        topo.put(types, l);
      }
//      int c = 0;
      for (int d = 1; d <= k0; d++) {
        for (String o : ordering.get(d)) {
          List<Integer> topInfo = topo.get(o);

          List<Integer> integers = querySign.get(nodeID);
          integers.add(topInfo == null ? 0 : topInfo.size());
//          if (topo.containsKey(o)) {
//            querySign[i][c++] = topInfo.size();
//          }
//          else
//            querySign[i][c++] = 0;
        }
      }
    }
  }

  private double getUpperbound(Set<Integer> consideredEdgeIndices, List<String> pc) {
    double score = 0;
    Set<Edge> coveredEdges = new HashSet<>();
    Map<Integer, Integer> map = new HashMap<>();
    Set<Integer> instantiatedVertices = new HashSet<>();
    for (int c : consideredEdgeIndices) {
      String e = actualQueryEdges.get(c);
      //int n1 = query.getInternalID(Integer.parseInt(e.split("#")[0]));
      //int n2 = query.getInternalID(Integer.parseInt(e.split("#")[1]));

      String[] split = e.split("#");
      int n1 = Integer.parseInt(split[0]);
      int n2 = Integer.parseInt(split[1]);

      Edge edge = new Edge(n1, n2, 1.0);
      coveredEdges.add(edge);
      edge = new Edge(n2, n1, 1.0);
      coveredEdges.add(edge);

      instantiatedVertices.add(n1);
      instantiatedVertices.add(n2);

      String[] split1 = pc.get(c).split("#");
      map.put(n1, Integer.parseInt(split1[0]));
      map.put(n2, Integer.parseInt(split1[1]));
    }

    Set<Path> globalPathSet = new HashSet<>();
    for (int i : instantiatedVertices)
      globalPathSet.addAll(getPaths(i, coveredEdges));
    //divide non-considered edges into paths and other edges.
    //compute all paths of length<k0 from each node in instantiated nodes.
    while (coveredEdges.size() != actualQueryEdges.size() * 2 && globalPathSet.size() != 0) {
      int maxLength = 0;
      List<Integer> maxPath = new ArrayList<>();
      for (Path p : globalPathSet) {
        if (p.nodes.size() > maxLength) {
          maxLength = p.nodes.size();
          maxPath = p.nodes;
        }
      }
      //add this maxPath to coveredEdges
      for (int ii = 1; ii < maxPath.size(); ii++) {
        Edge ee = new Edge(maxPath.get(ii), maxPath.get(ii - 1), 1.0);
        coveredEdges.add(ee);
        ee = new Edge(maxPath.get(ii - 1), maxPath.get(ii), 1.0);
        coveredEdges.add(ee);
      }
      //compute upperbound for this path
      String typeStr = "";
      for (int ll = 1; ll < maxPath.size(); ll++) {
        // typeStr += queryNodeID2Type.get(maxPath.get(ll));
        typeStr += queryNode2Type.get(maxPath.get(ll));
      }

      //score+=spd[map.get(maxPath.get(0))-1][orderingType2Index.get(typeStr)];
      //  score += spd[g.getInternalID(map.get(maxPath.get(0)))][orderingType2Index.get(typeStr)];
      score += spd.get(map.get(maxPath.get(0))).get(orderingType2Index.get(typeStr));

      //remove paths in globalPath containing edges on globalPath.
      HashSet<Path> globalPathSetNew = new HashSet<>();
      for (Path p : globalPathSet) {
        int flag = 0;
        for (int ii = 1; ii < p.nodes.size(); ii++) {
          for (int jj = 1; jj < maxPath.size(); jj++) {
            // if (maxPath.get(jj) == p.nodes.get(ii) && maxPath.get(jj - 1) == p.nodes.get(ii - 1)) {
            if (maxPath.get(jj).equals(p.nodes.get(ii)) &&
                maxPath.get(jj - 1).equals(p.nodes.get(ii - 1))) {
              flag = 1;
              break;
            }
          }
        }
        if (flag == 0)
          globalPathSetNew.add(p);
      }
      globalPathSet = globalPathSetNew;
    }
    //compute uncovered edges and account for them separately
    for (String edge : queryEdgetoIndex.keySet()) {
      String[] split = edge.split("#");
//      int n1 = query.getInternalID(Integer.parseInt(split[0]));
//      int n2 = query.getInternalID(Integer.parseInt(split[1]));
      int n1 = Integer.parseInt(split[0]);
      int n2 = Integer.parseInt(split[1]);
      Edge e = new Edge(n1, n2, 1.0);
      if (!coveredEdges.contains(e)) {
        String tmp = queryEdge2EdgeType.get(edge);
        if (!pointers.containsKey(tmp))
          tmp = tmp.split("#")[1] + "#" + tmp.split("#")[0];
        score += Double.parseDouble(sortedEdgeLists.get(tmp).get(pointers.get(tmp)).split("#")[2]);
      }
    }
    return score;
  }

  private Set<Path> getPaths(int i, Set<Edge> coveredEdges) {
    Set<Path> set = new HashSet<>();
    Path sp = new Path();
    sp.nodes.add(i);
    set.add(sp);
    List<Integer> currList = new ArrayList<>();
    Map<Integer, Integer> considered = new HashMap<>();
    considered.put(i, 1);
    currList.add(i);
    for (int k = 0; k < k0; k++) {
      List<Integer> newList = new ArrayList<>();
      Set<Integer> newListCopy = new HashSet<>();
      for (int n : currList) {
        //   ArrayList<Edge> nbrs = query.inLinks.get(n);
        Collection<Edge> nbrs = query.getNeighbors(n);
        for (Edge e : nbrs) {
          if ((!considered.containsKey(e.getSrc()) && !newListCopy.contains(e.getSrc())) || considered.get(e.getSrc()) == k + 1) {
            if (!considered.containsKey(e.getSrc()) && !newListCopy.contains(e.getSrc())) {
              newList.add(e.getSrc());
              newListCopy.add(e.getSrc());
              considered.put(e.getSrc(), k + 1);
            }
            if (coveredEdges.contains(e)) //if the edge has been covered already, you don't want to have a path containing that edge.
              continue;
            Set<Path> extras = new HashSet<>();
            for (Path p : set) {
              if (p.nodes.get(p.nodes.size() - 1) == n) {
                Path q = p.copyPath();
                q.nodes.add(e.getSrc());
                extras.add(q);
              }
            }
            for (Path p : extras)
              set.add(p);
          }
        }
      }
      currList = newList;
    }
    set.remove(sp);
    return set;
  }

  /**
   * Load types file
   *
   * @param typesFile
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
/*
  public void loadTypesFile()
      throws IOException {

    //load types file
    BufferedReader in = new BufferedReader(new FileReader(new File(baseDir, typesFile)));
    String str = "";
    while ((str = in.readLine()) != null) {
      String tokens[] = str.split("\\t");
      int node = Integer.parseInt(tokens[0]);
      int type = Integer.parseInt(tokens[1]);
      node2Type.put(node, type);
      if (type > totalTypes)
        totalTypes = type;
    }
    in.close();
  }
*/


  /**
   * Load types info from database
   *
   * @param dbConnection
   * @return
   * @throws Exception
   */
  public void loadTypesFromDatabase(DBConnection dbConnection, String tableName, String uidColumn, String typeColumn)
      throws Exception {
    Connection connection = dbConnection.getConnection();
    loadTypesFromDatabase(connection, tableName, uidColumn, typeColumn);
  }


  /**
   * Load types info from database
   *
   * @param connection
   * @param tableName
   * @param uidColumn
   * @param typeColumn
   * @throws Exception
   */
  public void loadTypesFromDatabase(Connection connection, String tableName, String uidColumn, String typeColumn)
      throws Exception {
		/*
		 * Do query
		 */
    String sqlQuery = "select " + uidColumn + ", " + typeColumn + " from " + tableName + ";";

    long then = System.currentTimeMillis();
    PreparedStatement queryStatement = connection.prepareStatement(sqlQuery);
    ResultSet rs = queryStatement.executeQuery();

		/*
		 * Loop-through result set, populate node2Type
		 */
    int c = 0;
    while (rs.next()) {
      c++;
      if (c % 100000 == 0) {
        logger.debug("loadTypesFromDatabase : read  " + c);
      }

      //Retrieve by column name
      int guid = rs.getInt(uidColumn);
      int type = rs.getInt(typeColumn);

      //logger.info("UID: "+guid+"\tTYPE: "+type);
      node2Type.put(guid, type);
      if (type > totalTypes)
        totalTypes = type;
    }

    long now = System.currentTimeMillis();

    logger.info("loadTypesFromDatabase got " + node2Type.size() + " from " + sqlQuery + " took " + (now-then) + " millis");

    rs.close();
    queryStatement.close();
  }

  /**
   * Figure out how many types there are (in the case where we're not loading everything from file)
   */
  public void computeTotalTypes() {
    for (int key : node2Type.keySet()) {
      int type = node2Type.get(key);
      if (type > totalTypes)
        totalTypes = type;
    }
  }

  /**
   * Setter for pre-loaded node2Type HashMap
   *
   * @see IngestAndQuery#executeQuery()
   */
  public void setNode2Type(Map<Integer, Integer> in) {
    node2Type = in;
  }

  /**
   * Getter for subgraph query results
   */
  public FibonacciHeap<List<String>> getHeap() {
    return heap;
  }

  /**
   * Setter for pre-loaded query-graph "query"
   */
/*
  public void setQueryGraph(Graph in) {
    query = in;
  }

  public void setQueryNodeID2Type(
      HashMap<Integer, Integer> queryNodeID2Type) {
    this.queryNodeID2Type = queryNodeID2Type;
  }

  public void setQueryNode2Type(HashMap<Integer, Integer> queryNode2Type) {
    this.queryNode2Type = queryNode2Type;
  }
*/
  public int getTotalTypes() {
    return totalTypes;
  }

  /**
   * Getter for queryEdgetoIndex
   *
   * @return
   * @see TopKSubgraphShortlist#getPatternSearchResults
   */
  public Map<String, Integer> getQueryEdgetoIndex() {
    return queryEdgetoIndex;
  }

/*
  public ArrayList<String> getActualQueryEdges() {
    return actualQueryEdges;
  }
*/
}

