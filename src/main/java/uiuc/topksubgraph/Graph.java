package uiuc.topksubgraph;

import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesBase;
import mitll.xdata.db.DBConnection;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Writer;
import java.sql.*;
import java.util.*;

/**
 * A graph is represented using edges, inLinks, outLinks.
 *
 * @author Manish Gupta (gupta58@illinois.edu)
 *         University of Illinois at Urbana Champaign
 */
public class Graph {
  private static final Logger logger = Logger.getLogger(Graph.class);

//  private int numNodes = 0;
//  private int numEdges = 0;
  private Set<Edge> edges;
  private Map<Integer, List<Edge>> inLinks;
  private final Map<Integer, Map<Integer, Edge>> inLinks2;

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
  private final Set<Integer> nodeIds = new HashSet<>();

  /**
   * Constructor
   */
  public Graph() {
    setEdges(new HashSet<>());
    setInLinks(new HashMap<>());
    inLinks2 = new HashMap<>();
  }

  public Graph(Map<Long, Integer> edgeToWeight) {
    this();
    simpleIds2(edgeToWeight);
    loadGraphFromMemory(edgeToWeight);
  }

/*  public Integer getInternalID(int rawID) {
    if (node2NodeIdMap == null) {
      logger.error("huh? node map is empty?");
    }

    Integer integer = node2NodeIdMap.get(rawID);
    return integer;// == null ? -1 : integer;
  }*/

  /**
   * TODO : why so schizo - everything internally should be in terms of internally assigned ids.
   *
   * Only when translating back into the outside world after we have results do we go back and translate IDs.
   *
   * @param internalID
   * @return
   *
   */
/*  public Integer getRawID(int internalID) {
    Integer integer = nodeId2NodeMap.get(internalID);
    return integer;
  }*/


  public Collection<Edge> getNeighbors(int n) {
    return inLinks.get(n);
  }

  /**
   * Returns the edge object if there exists an edge between node1 and node2 else returns null
   *
   * @param node1
   * @param node2
   * @return
   * @see #createRandomGraph(boolean)
   */
  protected Edge getEdge(int node1, int node2) {
    if (inLinks.containsKey(node2)) {
      List<Edge> a = inLinks.get(node2);
      for (Edge edge : a) {
        if (edge.getSrc() == node1)
          return edge;
      }
    }
    return null;
  }

  /**
   * @param node1
   * @param node2
   * @return
   * @see MultipleIndexConstructor#processPathsq(Writer, Writer, Graph, int)
   */
  public Edge getEdgeFast(int node1, int node2) {
    if (inLinks2.containsKey(node2)) {
      Map<Integer, Edge> integerEdgeMap = inLinks2.get(node2);
      return integerEdgeMap.get(node1);
    } else {
      return null;
    }
  }

  /**
   * Adds edge to graph considering direction from a to b
   *
   * @param a
   * @param b
   * @param weight
   */
  protected void addEdge(int a, int b, double weight) {
    Edge e = new Edge(a, b, weight);
    if (!edges.contains(e)) {
      edges.add(e);
//       logger.info("addEdge adding " + e);
     List<Edge> al = inLinks.get(b);

      if (inLinks.get(b) == null) {
        al = new ArrayList<>();
        inLinks.put(b, al);
      }

      al.add(e);

      Map<Integer, Edge> integerEdgeMap = inLinks2.get(b);
      if (integerEdgeMap == null) {
        integerEdgeMap = new HashMap<>();
        inLinks2.put(b, integerEdgeMap);
      }

      integerEdgeMap.put(a, e);
    }
  }

  /**
   * Loads the graph from an h2 database
   *
   * @param dbConnection
   * @throws Throwable
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#computeIndices(String, DBConnection)
   * @deprecated
   */
  public void loadGraph(DBConnection dbConnection, String tableName, String edgeName) throws SQLException {
    loadGraph(dbConnection.getConnection(), tableName, edgeName);
  }

  /**
   * Loads the graph from an h2 database
   *
   * @throws SQLException
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#computeIndicesFromMemory(String, DBConnection, Map)
   */
  public void loadGraphFromMemory(Map<Long, Integer> edgeToWeight) {
//    int nodeCount = 0;//node2NodeIdMap.size();
//    setNumNodes(0);
//    setNumEdges(0);

    logger.info("loadGraphFromMemory map " + 0 + " vs " + edgeToWeight.size());

    int c = 0;
    for (Map.Entry<Long, Integer> edgeAndCount : edgeToWeight.entrySet()) {
      Long key = edgeAndCount.getKey();

      int from = BitcoinFeaturesBase.getLow(key);
      int to   = BitcoinFeaturesBase.getHigh(key);
      int weight = edgeAndCount.getValue();

      // if (c < 20)
      // logger.info("loadGraphFromMemory " + from + " -> " + to + " = " + weight);

      c++;
      if (c % 1000000 == 0) {
        logger.debug("loadGraphFromMemory read  " + c);
        BitcoinFeaturesBase.rlogMemory();
      }
/*
      if (node2NodeIdMap.containsKey(from))
        from = node2NodeIdMap.get(from);
      else {
        node2NodeIdMap.put(from, nodeCount);
        nodeId2NodeMap.put(nodeCount, from);
        from = nodeCount;
        nodeCount++;
      }

      if (node2NodeIdMap.containsKey(to))
        to = node2NodeIdMap.get(to);
      else {
        node2NodeIdMap.put(to, nodeCount);
        nodeId2NodeMap.put(nodeCount, to);
        to = nodeCount;
        nodeCount++;
      }*/

      addNodeAndEdge(from, to, weight);
    }

//    setNumNodes(nodeIds.size());
//    setNumEdges(c);
  }

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

  public void simpleIds2(Map<Long, Integer> edgeToWeight) {
    //Map<Integer,Integer> extToInternal = new HashMap<>();
    int count = 0;
    for (Long key : edgeToWeight.keySet()) {
      int from = BitcoinFeaturesBase.getLow(key);
      int to   = BitcoinFeaturesBase.getHigh(key);

/*      Integer finternal = node2NodeIdMap.get(from);
      if (finternal == null) {
        finternal = count++;
        node2NodeIdMap.put(from, finternal);
        nodeId2NodeMap.put(finternal, from);
      }

      Integer tinternal = node2NodeIdMap.get(to);
      if (tinternal == null) {
        tinternal = count++;
        node2NodeIdMap.put(to, tinternal);
        nodeId2NodeMap.put(tinternal, to);
      }*/

      nodeIds.add(from);
      nodeIds.add(to);

    }
  }

  /**
   * Loads the graph from an h2 database
   *
   * @param connection
   * @param tableName
   * @throws SQLException
   * @see Graph#loadGraph(DBConnection, String, String)
   */
  public void loadGraphAgain(Connection connection, String tableName) throws SQLException {
   // int nodeCount = node2NodeIdMap.size();
//    setNumNodes(0);
//    setNumEdges(0);

		/*
     * Do database query
		 */
    String sqlQuery = "select * from " + tableName;

    logger.info("loadGraphAgain doing " + sqlQuery);
    PreparedStatement queryStatement = connection.prepareStatement(sqlQuery);
    ResultSet rs = queryStatement.executeQuery();

		/*
     * Loop through database table rows...
		 */
    int c = 0;
    while (rs.next()) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " + c);

      int i = 1;
      int from = rs.getInt(i++);
      int to = rs.getInt(i++);
      int weight = rs.getInt(i++);

/*      if (node2NodeIdMap.containsKey(from))
        from = node2NodeIdMap.get(from);
      else {
        node2NodeIdMap.put(from, nodeCount);
        nodeId2NodeMap.put(nodeCount, from);
        from = nodeCount;
        nodeCount++;
      }
      if (node2NodeIdMap.containsKey(to))
        to = node2NodeIdMap.get(to);
      else {
        node2NodeIdMap.put(to, nodeCount);
        nodeId2NodeMap.put(nodeCount, to);
        to = nodeCount;
        nodeCount++;
      }*/

      addNodeAndEdge(from, to, weight);
    }

    rs.close();
    queryStatement.close();

//    setNumNodes(nodeIds.size());
//    setNumEdges(c);

    logger.info("read " + c + " found " + getNumNodes() + " nodes and " + getNumEdges() + " edges");
  }

  /**
   * Loads the graph from an h2 database
   *
   * @param connection
   * @param tableName
   * @param edgeName
   * @throws SQLException
   * @see Graph#loadGraph(DBConnection, String, String)
   */
  private void loadGraph(Connection connection, String tableName, String edgeName) throws SQLException {
   // int nodeCount = node2NodeIdMap.size();
//    setNumNodes(0);
//    setNumEdges(0);
		
		/*
		 * Do database query
		 */
    //Connection connection = dbConnection.getConnection();

    String sqlQuery = "select * from " + tableName + ";";

    PreparedStatement queryStatement = connection.prepareStatement(sqlQuery);
    ResultSet rs = queryStatement.executeQuery();
		
		/*
		 * Loop through database table rows...
		 */
    int c = 0;
    while (rs.next()) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " + c);

      Array array = rs.getArray("sorted_pair");
      Object rsArray = array.getArray();
      Object[] sortedPair = (Object[]) rsArray;

      double weight = rs.getDouble(edgeName);

//			double tot_out = rs.getDouble("tot_out");
//			double tot_in = rs.getDouble("tot_in");
//			double tot_usd = rs.getDouble("tot_usd");
//			double num_trans = rs.getDouble("num_trans");
//			weight = tot_usd/num_trans;
//			weight = 1;

      int from = (Integer) sortedPair[0];
      int to = (Integer) sortedPair[1];
//			long from = (Long)sortedPair[0];
//			long to = (Long)sortedPair[1];

      //logger.info("First node is: "+from+" Second node is: "+to+" Number of trans: "+weight);

/*      if (node2NodeIdMap.containsKey(from))
        from = node2NodeIdMap.get(from);
      else {
        node2NodeIdMap.put(from, nodeCount);
        nodeId2NodeMap.put(nodeCount, from);
        from = nodeCount;
        nodeCount++;
      }
      if (node2NodeIdMap.containsKey(to))
        to = node2NodeIdMap.get(to);
      else {
        node2NodeIdMap.put(to, nodeCount);
        nodeId2NodeMap.put(nodeCount, to);
        to = nodeCount;
        nodeCount++;
      }*/

//			if (tot_out == 0.0) {this.addEdge(to,from,weight);}
//			if (tot_in == 0.0) {this.addEdge(from,to,weight);}
//			
//			if (tot_out != 0.0 && tot_in != 0.0) {
//				this.addEdge(from, to, weight);
//				this.addEdge(to, from, weight);
//			}

      addNodeAndEdge(from, to, weight);
    }

    rs.close();
    queryStatement.close();

//    setNumNodes(nodeIds.size());
//    setNumEdges(c);
  }

  private void addNodeAndEdge(int from, int to, double weight) {
    nodeIds.add(from);
    nodeIds.add(to);
    this.addEdge(from, to, weight);
    this.addEdge(to, from, weight);
  }

  /**
   * Loads the graph from a HashSet of edges
   *
   * @param edges
   */
  public void loadGraph(Collection<Edge> edges) {
//    int nodeCount = node2NodeIdMap.size();
//    setNumNodes(0);
//    setNumEdges(0);
		
		/*
		 * Loop through edges...
		 */
    int c = 0;
    for (Edge edg : edges) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " + c);

      //get edge information
      int from = edg.getSrc();
      int to = edg.getDst();
      double weight = edg.getWeight();

      //logger.info("First node is: "+from+" Second node is: "+to+" Number of trans: "+weight);

/*      if (node2NodeIdMap.containsKey(from))
        from = node2NodeIdMap.get(from);
      else {
        node2NodeIdMap.put(from, nodeCount);
        nodeId2NodeMap.put(nodeCount, from);
        from = nodeCount;
        nodeCount++;
      }
      if (node2NodeIdMap.containsKey(to))
        to = node2NodeIdMap.get(to);
      else {
        node2NodeIdMap.put(to, nodeCount);
        nodeId2NodeMap.put(nodeCount, to);
        to = nodeCount;
        nodeCount++;
      }*/

      addNodeAndEdge(from, to, weight);
    }

//    setNumNodes(nodeCount);
//    setNumEdges(c);

    report();
  }

  private void report() {
    logger.info("numNodes: " + getNumNodes());
    logger.info("numEdges: " + getNumEdges());
  }

  /**
   * Loads the graph from a file
   *
   * @param f
   * @throws Throwable
   */
  public void loadGraph(File f) throws Throwable {
    BufferedReader in = new BufferedReader(new FileReader(f));
    String str = "";
//    int nodeCount = node2NodeIdMap.size();
    while ((str = in.readLine()) != null) {
      if (str.startsWith("#")) {
        if (str.contains("#Nodes")) {
//          setNumNodes(Integer.parseInt(str.split("\\s+")[1].trim()));
//          setNumEdges(Integer.parseInt(in.readLine().split("\\s+")[1].trim()));
        }
        //System.err.println(str);
        continue;
      }
      String tokens[] = str.split("#|\\t");
      int from = Integer.parseInt(tokens[0]);
      int to = Integer.parseInt(tokens[1]);
/*      if (node2NodeIdMap.containsKey(from))
        from = node2NodeIdMap.get(from);
      else {
        node2NodeIdMap.put(from, nodeCount);
        nodeId2NodeMap.put(nodeCount, from);
        from = nodeCount;
        nodeCount++;
      }
      if (node2NodeIdMap.containsKey(to))
        to = node2NodeIdMap.get(to);
      else {
        node2NodeIdMap.put(to, nodeCount);
        nodeId2NodeMap.put(nodeCount, to);
        to = nodeCount;
        nodeCount++;
      }*/
      if (tokens.length > 2) {
        this.addEdge(from, to, Double.parseDouble(tokens[2]));
        this.addEdge(to, from, Double.parseDouble(tokens[2]));
      } else {
        this.addEdge(from, to, 1);
        this.addEdge(to, from, 1);
      }
    }
    in.close();
  }


  public int getNumNodes() {
    return nodeIds.size();
  }
/*

  protected void setNumNodes(int numNodes) {
    this.numNodes = numNodes;
  }
*/

  public int getNumEdges() {
    return edges.size();
  }

//  protected void setNumEdges(int numEdges) {
//    this.numEdges = numEdges;
//  }

  public Set<Edge> getEdges() {
    return edges;
  }

  protected void setEdges(Set<Edge> edges) {
    this.edges = edges;
  }

  protected  Map<Integer, List<Edge>> getInLinks() {
    return inLinks;
  }

  public Set<Integer> getInLinksNodes() { return inLinks.keySet(); }

  protected void setInLinks(Map<Integer, List<Edge>> inLinks) {
    this.inLinks = inLinks;
  }

/*
  private HashMap<Integer, Map<Integer, Edge>> getInLinks2() {
    return inLinks2;
  }
*/

  public Collection<Integer> getRawIDs() {
    return nodeIds;// node2NodeIdMap.keySet();
  }

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
    return "Graph with " + getNumNodes() + " nodes and " +getNumEdges() + " edges "//null " + isNull
        //+ " " + (isNull ? "" : node2NodeIdMap.keySet())
        ;
  }
}
