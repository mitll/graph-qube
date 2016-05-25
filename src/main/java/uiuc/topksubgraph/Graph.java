package uiuc.topksubgraph;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * A graph is represented using edges, inLinks, outLinks.
 *
 * @author Manish Gupta (gupta58@illinois.edu)
 *         University of Illinois at Urbana Champaign
 */
public class Graph {
  private static final Logger logger = Logger.getLogger(Graph.class);

  protected Set<Edge> edges = new HashSet<>();
  protected Map<Long, List<Edge>> inLinks = new HashMap<>();
  protected final Map<Long, Map<Long, Edge>> inLinks2 = new HashMap<>();
  boolean populateInLinks2 = false;

  private final Map<Long, Integer> node2Type = new HashMap<>();

  /**
   * Maps the node to an internal node id.
   */
  // private final Map<Integer, Integer> node2NodeIdMap = new HashMap<>();

  /**
   * Maps internal node id to a node
   * <p>
   * TODO: this is dumb - this is just an integer array!!!
   */
//  private final Map<Integer, Integer> nodeId2NodeMap = new HashMap<>();
   final Set<Long> nodeIds = new HashSet<>();

  /**
   * Constructor
   */
/*
  public Graph() {
    setEdges(new HashSet<>());
    setInLinks(new HashMap<>());
    inLinks2 = new HashMap<>();
  }
*/

/*  public Graph(Map<Long, Integer> edgeToWeight,boolean populateInLinks2) {
    this();
    this.populateInLinks2 = populateInLinks2;
    simpleIds2(edgeToWeight);
    loadGraphFromMemory(edgeToWeight);
  }*/

/*  public Integer getInternalID(int rawID) {
    if (node2NodeIdMap == null) {
      logger.error("huh? node map is empty?");
    }

    Integer integer = node2NodeIdMap.get(rawID);
    return integer;// == null ? -1 : integer;
  }*/

  /**
   * TODO : why so schizo - everything internally should be in terms of internally assigned ids.
   * <p>
   * Only when translating back into the outside world after we have results do we go back and translate IDs.
   *
   * @return
   * @paramx internalID
   */
/*  public Integer getRawID(int internalID) {
    Integer integer = nodeId2NodeMap.get(internalID);
    return integer;
  }*/
   Collection<Edge> getNeighbors(long n) {
    return inLinks.get(n);
  }

  /**
   * Returns the edge object if there exists an edge between node1 and node2 else returns null
   *
   * @param node1
   * @param node2
   * @return
   * @see QueryExecutor#getQueryEdges(List, Graph)
   */
   Edge getEdge(long node1, long node2) {
    if (inLinks.containsKey(node2)) {
      List<Edge> a = inLinks.get(node2);
      for (Edge edge : a) {
        if (edge.getSrc() == node1)
          return edge;
      }
    }
    return null;
  }

  public boolean areConnected(int node1, int node2) {
    boolean b = getEdge(node1, node2) != null;
    if (!b && getEdge(node2, node1) != null)
      logger.error("huh? thought graph was undirected " + getEdge(node1, node2) +
          " but " + getEdge(node2, node1));
    return b;
  }

  /**
   * ONLY WORKS if populateInLinks2 is true
   * @param node1
   * @param node2
   * @return
   * @see MultipleIndexConstructor#processPathsq
   */
   Edge getEdgeFast(long node1, long node2) {
    if (inLinks2.containsKey(node2)) {
      Map<Long, Edge> integerEdgeMap = inLinks2.get(node2);
      return integerEdgeMap.get(node1);
    } else {
      return null;
    }
  }

  /*  private NodeInfo getNodeInfo(Integer rawID) {
    NodeInfo nodeInfo = new NodeInfo(rawID);
    Collection<Edge> values = getUniqueEdges(rawID);

    for (Edge edge : values) {
      int src = edge.getSrc();
      float weight = edge.getFWeight();

//        logger.info("edge " + edge + " src " + src);
      String types = oneHop[getNodeType(src) - 1];
      nodeInfo.addWeight(types, weight, src);

//      if (DEBUG) {
//        logger.info("d1 : for " + count + " : " + edge + " src " +
//            src + " node " + nodeInfo);
//      }
    }
    return nodeInfo;
  }*/

/*  public Collection<Edge> getUniqueEdges(int i) {
    Collection<Edge> nbrs = getNeighbors(i);
    // Set<Integer> seen = new HashSet<>();

    Map<Integer, Edge> srcToEdge = new HashMap<>();
    Map<Integer, Float> srcToWeight = new HashMap<>();

    for (Edge edge : nbrs) {
      int src = edge.getSrc();
      //int srcNode = graph.getRawID(src);
      Float edgeWeight = srcToWeight.get(src);
      float fWeight = edge.getFWeight();
      if (edgeWeight == null || edgeWeight < fWeight) {
        srcToWeight.put(src, fWeight);
        srcToEdge.put(src, edge);
      }
    }

    return srcToEdge.values();
  }*/

  /*  private void simpleIds(Map<Long, Integer> edgeToWeight) {
    for (Long key : edgeToWeight.keySet()) {
      int from = BitcoinFeaturesBase.getLow(key);
      int to = BitcoinFeaturesBase.getHigh(key);

      node2NodeIdMap.put(from, from);
      node2NodeIdMap.put(to, to);

      nodeId2NodeMap.put(from, from);
      nodeId2NodeMap.put(to, to);
    }
  }*/

  protected void report() {
    logger.info("numNodes: " + getNumNodes());
    logger.info("numEdges: " + getNumEdges());
  }


  public int getNumNodes() {
    return nodeIds.size();
  }

  public int getNumEdges() {
    return edges.size();
  }

  public Set<Edge> getEdges() {
    return edges;
  }

  private void setEdges(Set<Edge> edges) {
    this.edges = edges;
  }

  protected Map<Long, List<Edge>> getInLinks() {
    return inLinks;
  }

  Set<Long> getInLinksNodes() {
    return inLinks.keySet();
  }

  private void setInLinks(Map<Long, List<Edge>> inLinks) {
    this.inLinks = inLinks;
  }

/*
  private HashMap<Integer, Map<Integer, Edge>> getInLinks2() {
    return inLinks2;
  }
*/

  public Collection<Long> getRawIDs() {
    return nodeIds;// node2NodeIdMap.keySet();
  }

  public Integer getNodeType(long src) {
    return node2Type.get(src);
  }

/*  public void makeTypeIDs(Collection<Integer> types) {
    logger.info("makeTypeIDs " + types);
    oneHop = new String[types.size()];
    twoHop = new String[types.size()][types.size()];

    int i = 0;
    for (Integer t : types) {
      String firstType = Integer.toString(t);
      oneHop[i] = firstType;
      int j = 0;
      for (Integer tt : types) {
        String secondType = Integer.toString(tt);
        //    twoHop[i][j++] = firstType+"_"+secondType;
        String both = firstType + secondType;
        twoHop[i][j++] = both;
        logger.info("makeTypeIDs added " + both);
      }
      i++;
    }

    logger.info("makeTypeIDs one " + Arrays.asList(oneHop));
    logger.info("makeTypeIDs two hop " + Arrays.asList(twoHop));
  }*/

  private static String[] oneHop;
  private static String[][] twoHop;

  /**
   * TODO this should be n - the size of the array
   *
   * @return
   */
/*  public Collection<Integer> getInternalIDs() {
    return nodeId2NodeMap.keySet();
  }*/
  public String toString() {
    //boolean isNull = node2NodeIdMap == null;
    return "Graph with " + getNumNodes() + " nodes and " + getNumEdges() + " edges "//null " + isNull
        //+ " " + (isNull ? "" : node2NodeIdMap.keySet())
        ;
  }
}
