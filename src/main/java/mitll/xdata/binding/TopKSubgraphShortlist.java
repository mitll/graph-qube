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

import influent.idl.*;
import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.dataset.bitcoin.ingest.StatementResult;
import mitll.xdata.db.DBConnection;
import org.apache.log4j.Logger;
import org.jgrapht.util.FibonacciHeap;
import org.jgrapht.util.FibonacciHeapNode;
import uiuc.topksubgraph.Edge;
import uiuc.topksubgraph.MutableGraph;
import uiuc.topksubgraph.QueryExecutor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Collator;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Uses the Top-K Subgraph Search algorithm in uiuc.topksubgraph
 * as shortlisting engine
 *
 * @author Charlie Dagli (dagli@ll.mit.edu)
 *         MIT Lincoln Laboratory
 */
public class TopKSubgraphShortlist extends Shortlist {
  private static final Logger logger = Logger.getLogger(TopKSubgraphShortlist.class);
  private static final String MARGINAL_GRAPH = "MARGINAL_GRAPH";
  public static final boolean SKIP_LINK_PROPERTIES = false;

  private int K = 50;
  private int D = 2;

  private String usersTable = "users";
  private String userIdColumn = "user";
  private String typeColumn = "type";

  private String graphTable = "MARGINAL_GRAPH";
  private String pairIDColumn = "sorted_pairr";
  private HashMap<String, String> edgeAttributeName2Type;

  private final QueryExecutor executor;

  /**
   * @param binding
   * @see mitll.xdata.dataset.bitcoin.binding.BitcoinBinding#BitcoinBinding
   */
  public TopKSubgraphShortlist(Binding binding) {
    super(binding);

    //setup shortlist stuff here having to do with state...
    logger.info("TopKSubgraphShortlist : setting up query executor... this should only happen once...");
    executor = new QueryExecutor();

    QueryExecutor.datasetId = binding.datasetId;
    QueryExecutor.baseDir = /*"src/main/resources" +*/ binding.datasetResourceDir; //THIS LINE SHOULD CHANGE FOR JAR-ed VERSION
    //QueryExecutor.k0 = D;
    //QueryExecutor.topK= K;

    //load graph (this may just need to be rolled up into BitcoinBinding)
    logger.info("TopKSubgraphShortlist Loading graph. This should only happen once...");
    MutableGraph g = null;
    try {
      //  g.loadGraph(binding.connection, "MARGINAL_GRAPH", "NUM_TRANS");
      logger.info("connection " + binding.connection);
      g = new MutableGraph(binding.connection, MARGINAL_GRAPH);
    } catch (Exception e) {
      logger.info("Got: " + e);
    }
    executor.g = g;

    // QueryExecutor.spathFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".spath";
    QueryExecutor.topologyFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".topology";
    QueryExecutor.spdFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".spd";
    QueryExecutor.resultDir = "results";

    windowsPathnamePortabilityCheck();
  }

  /**
   * Method to set algorithm parameters once they've been set externally
   */
  public void refreshQueryExecutorParameters() {
    QueryExecutor.k0 = D;
    QueryExecutor.topK = K;
  }

  /**
   * Load a whole bunch of stuff from:
   * db: node type data
   * file: and all pre-computed indices
   *
   * @see BitcoinBinding#BitcoinBinding(DBConnection, boolean, String)
   */
  public void loadTypesAndIndices() {
    /*
     * Load-in types, and count how many there are
		 */
    try {
      logger.info("Loading in types...");
      executor.loadTypesFromDatabase(binding.connection, usersTable, userIdColumn, typeColumn);

      logger.info("Loading in indices...");
      executor.prepareInternals();

      //Make resultDir if necessary
      File directory = new File(QueryExecutor.baseDir + QueryExecutor.resultDir);
      if (!directory.exists() && !directory.mkdirs())
        throw new IOException("Could not create directory: " + QueryExecutor.baseDir + QueryExecutor.resultDir);

    } catch (IOException e) {
      logger.info("Got IOException: " + e);
    } catch (Exception e) {
      logger.info("Got Exception: " + e);
    } catch (Throwable e) {
      logger.info("Got Throwable: " + e);
    }

    // Get name and type of edge attributes in graph
    edgeAttributeName2Type = getGraphEdgeAttributeName2Type();
  }


  /**
   * @param entityMatchDescriptorsIgnored
   * @param exemplarIDs
   * @param max
   * @return
   * @see Binding#getShortlist(
   */
  @Override
  public List<FL_PatternSearchResult> getShortlist(List<FL_EntityMatchDescriptor> entityMatchDescriptorsIgnored,
                                                   List<String> exemplarIDs, long max) {
    // which binding are we bound to?
    logger.info(this.binding.toString());
    logger.info(this.binding.connection);

    //check to see if we can connect to anything....
    if (existsTable(graphTable)) {
//      logger.info("we can connect to the right database...");
    } else {
      logger.info("table " + graphTable + " does not yet exist.");
    }

		/*
     * Get all pairs of query nodes...
		 * (this is assuming ids are sortable by integer comparison, like in bitcoin)
		 */
    Collection<Edge> queryEdges = getQueryEdges(exemplarIDs);

    for (Edge qe : queryEdges) {
      logger.info("getShortlist qe: " + qe);
    }
    long then = System.currentTimeMillis();
    boolean isClique = executor.loadQuery(queryEdges, binding.connection, usersTable, userIdColumn, typeColumn);
    if (isClique) logger.info("getShortlist isClique!");
    long now = System.currentTimeMillis();
    logger.info("took " + (now - then) + " millis to load query");

    //set system out to out-file...
    QueryExecutor.queryFile = "queries/queryGraph.FanOutService.txt";
    try {
      System.setOut(new PrintStream(new File(QueryExecutor.baseDir + QueryExecutor.resultDir +
          "/QueryExecutor.topK=" + QueryExecutor.topK +
          "_K0=" + QueryExecutor.k0 +
          "_" + QueryExecutor.datasetId +
          "_servicetest" +
          "_" + QueryExecutor.queryFile.split("/")[1])));
    } catch (FileNotFoundException e) {
      logger.info("got: " + e);
    }
    if (!queryEdges.isEmpty()) {
      then = System.currentTimeMillis();
      executor.executeQuery(isClique);
      now = System.currentTimeMillis();
      logger.info("getShortlist took " + (now - then) + " millis to execute query");
    }

    logger.info("getShortlist : original entity ordering from Influent query: " + exemplarIDs);
    return getPatternSearchResults(queryEdges, exemplarIDs, (int)max);
  }

  /**
   * Ask the transactions table for edges between the node ids.
   *
   * @param exemplarIDs
   * @return
   * @see #getShortlist(List, List, long)
   */
  private Collection<Edge> getQueryEdges(List<String> exemplarIDs) {
    Set<Edge> queryEdges = new HashSet<Edge>();

//    logger.info("getQueryEdges : ran on " + exemplarIDs);

    if (exemplarIDs.size() > 1) {
      for (int i = 0; i < exemplarIDs.size(); i++) {
        for (int j = i + 1; j < exemplarIDs.size(); j++) {
          int e1 = Integer.parseInt(exemplarIDs.get(i));
          int e2 = Integer.parseInt(exemplarIDs.get(j));

          if (executor.g.areConnected(e1, e2)) {
            Edge edg;
            if (e1 <= e2) {
              //  pair = "(" + e1 + "," + e2 + ")";
              edg = new Edge(e1, e2, 1.0);  //put in here something to get weight if wanted...
            } else {
              //  pair = "(" + e2 + "," + e1 + ")";
              edg = new Edge(e2, e1, 1.0);  //put in here something to get weight if wanted...
            }

            queryEdges.add(edg);
          } else {
            logger.warn("getQueryEdges: no edge between " + e1 + " and " + e2);

          }
        }
      }
    }
    return queryEdges;
  }

  /**
   * @param tableName
   */
  private boolean existsTable(String tableName) {
    try {
      String sql = "select 1 from " + tableName + " limit 1;";
      StatementResult statementResult = doSQLQuery(sql);
      statementResult.close();
      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * Do SQL, query something, return that something
   *
   * @param sql
   * @return
   * @throws SQLException
   */
  private StatementResult doSQLQuery(String sql) throws SQLException {
    PreparedStatement queryStatement = this.binding.connection.prepareStatement(sql);
    ResultSet rs = queryStatement.executeQuery();
    return new StatementResult(rs, queryStatement);
  }


  /**
   * @param k
   */
  public void setK(int k) {
    K = k;
  }

  /**
   * @param d
   */
  public void setD(int d) {
    D = d;
  }

  /**
   * @param usersTable
   */
  public void setUsersTable(String usersTable) {
    this.usersTable = usersTable;
  }


  /**
   * @param typeColumn
   */
  public void setTypeColumn(String typeColumn) {
    this.typeColumn = typeColumn;
  }

  /**
   * @param userIdColumn
   */
  public void setUserIdColumn(String userIdColumn) {
    this.userIdColumn = userIdColumn;
  }

  /**
   * @param graphTable
   */
  public void setGraphTable(String graphTable) {
    this.graphTable = graphTable;
  }

  /**
   * @param pairIDColumn
   */
  public void setPairIDColumn(String pairIDColumn) {
    this.pairIDColumn = pairIDColumn;
  }


  /**
   * Convert candidate graphs into pattern search results.
   *
   * @param queryEdges
   * @return
   * @see #getShortlist(List, List, long)
   */
  private List<FL_PatternSearchResult> getPatternSearchResults(Collection<Edge> queryEdges, List<String> exemplarIDs, int max) {

    // Heap of results from uiuc.topksubgraph
    FibonacciHeap<List<String>> heap = executor.getHeap();

    // List of type FL_PatternSearchResult to hold all matching sub-graph contained in heap
    List<FL_PatternSearchResult> results = new ArrayList<FL_PatternSearchResult>();

		/*
     * Map query node names to their corresponding entity ind from the Influent JSON
		 */
    Map<String, Integer> queryNode2InfluentEntityInd = new HashMap<String, Integer>();
    int i = 0;
    for (String node : exemplarIDs) {
      queryNode2InfluentEntityInd.put(node, i);
      i++;
    }

		/*
     * Map between influent and uiuc.topksubraph index structure for query edges
		 */
    Map<String, Integer> uiucQueryEdgetoIndex = executor.getQueryEdgetoIndex();
    //Map<Integer, Integer> uiucInd2InfluentInd = new HashMap<Integer, Integer>();

    i = 0;
    for (Edge qe : queryEdges) {
      //assuming here qe.src and qe.dst are ordered properly (i.e. src < dst)
      String edgeString = qe.getSrc() + "#" + qe.getDst();
      int uiucInd = uiucQueryEdgetoIndex.get(edgeString);
      // uiucInd2InfluentInd.put(uiucInd, i);
      i++;
    }

    // Compute map between influent entity ind and list of edges
    Map<SortedSet<String>, Integer> queryEdgeList2InfluentEntityInd =
        computeQueryEdgeList2InfluentEntityIndMap(/*exemplarIDs,*/ queryNode2InfluentEntityInd, uiucQueryEdgetoIndex);

    logger.info("getPatternSearchResults queryEdgeList2InfluentEntityInd " + queryEdgeList2InfluentEntityInd.size());
    /*
     * Convert query to FL_PatternSearchResult and add to results...
		 */
    // FL_PatternSearchResult queryAsResult = makeQueryIntoResult(exemplarIDs, uiucQueryEdgetoIndex);
    results.add(makeQueryIntoResult(exemplarIDs, uiucQueryEdgetoIndex));

    // Set of unique subgraphs found
    Set<String> uniqueSubgraphGuids = new HashSet<String>();

    logger.info("getPatternSearchResults : Starting with: " + heap.size() + " matching sub-graphs...");

    // Loop-through resultant sub-graphs
    while (!heap.isEmpty()) {
      // List of type FL_EntityMatchResult for entities involved in this sub-graph
      List<FL_EntityMatchResult> entities = new ArrayList<FL_EntityMatchResult>();

      // Get matching sub-graph
      FibonacciHeapNode<List<String>> fhn = heap.removeMin();
      List<String> list = fhn.getData();

      // Sub-graph score
      double subgraphScore = fhn.getKey();

      // HashSet for entities involved in this sub-graph
      Set<String> nodes = getSubgraphNodes(list);

      // make subgraph guid
      String subgraphGuid = getSubgraphGuid(nodes);

//      logger.debug("got match " + nodes);

      // Proceed only if we've not seen this graph previously...
      if (!uniqueSubgraphGuids.contains(subgraphGuid)) {
        // add this subgraph
        uniqueSubgraphGuids.add(subgraphGuid);
        //logger.info(subgraphGuid);
        //logger.info("original nodes: "+nodes);

				/*
         *  Loop through all edges
				 */
        // List of type FL_LinkMatchResult to store all edge information from subgraph
        List<FL_LinkMatchResult> links = new ArrayList<>();
//        for (i = 0; i < list.size(); i++)
//          links.add(new FL_LinkMatchResult());

        for (String edgeInfo : list) {
          addLinkMatchResultForEdge(nodes, links, edgeInfo);
        }
  //      logger.info("links: "+links);
        //logger.info(queryEdges);
        //logger.info(executor.getActualQueryEdges());
    //    logger.info("list: "+list);
        logger.info("final nodes: "+nodes);
				
				
				/*
				 * Get mapping between nodes and all edges they are involved in (for node correspondence mapping b/w query and result)
				 */
        Map<String, SortedSet<String>> resultNode2EdgeList = computeResultNode2EdgeListMap(list/*, nodes.size()*/);

				/*
				 * Loop through all nodes
				 */
        for (String node : nodes) {
          FL_EntityMatchResult entityMatchResult = makeEntityMatchResult(queryEdgeList2InfluentEntityInd, resultNode2EdgeList, node);
          entities.add(entityMatchResult);
        }
        //logger.info(entities);

				/*
				 * Set score,entities,links to result
				 */
        FL_PatternSearchResult result = new FL_PatternSearchResult();
        result.setScore(subgraphScore);
        result.setEntities(entities);
        result.setLinks(links);
        //logger.info(result);

        results.add(result);

        if (results.size() == 10) break;
      }
    }

    logger.info("getPatternSearchResults Ending with: " + uniqueSubgraphGuids.size() + " unique matching sub-graphs...");

    logger.info("# results :    " +results.size());
//    logger.info("second  " +results.get(1));

    return results;
  }

  /*
   * Process the edge to build FL_Link
   */
  private void addLinkMatchResultForEdge(Set<String> nodes, List<FL_LinkMatchResult> links, String edgeInfo) {
    // Container to hold FL_Link (has other metadata involved)
    FL_LinkMatchResult linkMatchResult = new FL_LinkMatchResult();

    // Get parts of link
    String[] edgeSplit = edgeInfo.split("#");
    String src = edgeSplit[0];
    String dest = edgeSplit[1];
    double edge_weight = Double.parseDouble(edgeSplit[2]);

    // Actual FL_Link
    FL_Link link = makeLink(src, dest);

    linkMatchResult.setScore(edge_weight);
    linkMatchResult.setUid("");
    linkMatchResult.setLink(link);
    //links.add(linkMatchResult);

					/*
					links.add(uiucInd2InfluentInd.get(i),linkMatchResult);
					links.remove(uiucInd2InfluentInd.get(i)+1); //a bit hokey...
					*/
    links.add(linkMatchResult);

					/*
					 * Track nodes (as currently written doesn't get into these conditionals..)
					 */
    nodes.add(src);
    nodes.add(dest);
  }

  private FL_Link makeLink(String src, String dest) {
    FL_Link link = new FL_Link();
    link.setSource(src);
    link.setTarget(dest);
    link.setUid("something");
    //link.setTags(new ArrayList<FL_LinkTag>()); //Deprecated in Influent IDL 2.0

    link.setProperties(makeLinkProperties(src, dest));
    return link;
  }

  Map<String, Map<String, List<FL_Property>>> srcToDestToProps = new HashMap<>();

  int num = 0;

  /**
   * TODO : Super slow???
   *
   * @param src
   * @param dest
   * @return
   */
  private List<FL_Property> makeLinkProperties(String src, String dest) {
    List<FL_Property> linkProperties;

    Map<String, List<FL_Property>> orDefault = srcToDestToProps.get(src);
    if (orDefault == null) srcToDestToProps.put(src, orDefault = new HashMap<String, List<FL_Property>>());
    List<FL_Property> orDefault1 = orDefault.get(dest);

    if (orDefault1 != null) return orDefault1;

    if (binding.compareEntities(src, dest) == 1) {
      linkProperties = getLinkProperties(dest, src);
    } else {
      linkProperties = getLinkProperties(src, dest);
    }
    orDefault.put(dest, linkProperties);

    if (++num % 100 == 0) logger.info("makeLinkProperties did " + num);
    return linkProperties;
  }

  private FL_EntityMatchResult makeEntityMatchResult(Map<SortedSet<String>, Integer> queryEdgeList2InfluentEntityInd,
                                                     Map<String, SortedSet<String>> resultNode2EdgeList,
                                                     String node) {
    int entityInd = queryEdgeList2InfluentEntityInd.get(resultNode2EdgeList.get(node));
    return makeEntityMatchResult(node, entityInd);
  }

  private FL_EntityMatchResult makeEntityMatchResult(String node, int entityInd) {
    FL_EntityMatchResult entityMatchResult = new FL_EntityMatchResult();

    entityMatchResult.setScore(1.0);
    entityMatchResult.setUid("E" + entityInd);

    //= binding.makeEntityMatchResult(exemplarQueryID, similarID, similarity);
    FL_Entity entity = makeEntity(node);

    // entity
    entityMatchResult.setEntity(entity);
    return entityMatchResult;
  }

  private FL_Entity makeEntity(String node) {
    FL_Entity entity = new FL_Entity();
    entity.setUid(node);
    entity.setTags(new ArrayList<FL_EntityTag>());

    List<FL_Property> entityProperties = new ArrayList<FL_Property>();
    // node_id
    FL_Property property = new FL_Property();
    property.setKey("node_id");
    property.setFriendlyText(node);
    property.setRange(new FL_SingletonRange(node, FL_PropertyType.STRING));
    property.setTags(new ArrayList<FL_PropertyTag>());
    entityProperties.add(property);
    // TYPE
    property = new FL_Property();
    property.setKey("TYPE");
    property.setFriendlyText("null");
    property.setRange(new FL_SingletonRange("null", FL_PropertyType.STRING));
    property.setTags(new ArrayList<FL_PropertyTag>());
    entityProperties.add(property);

    entity.setProperties(entityProperties);
    return entity;
  }


  /**
   * Convert query graphs into pattern search "result".
   *
   * @see #getPatternSearchResults(Collection, List)
   * @param exemplarIDs
   * @return
   */
  private FL_PatternSearchResult makeQueryIntoResult(List<String> exemplarIDs, Map<String, Integer> uiucQueryEdgetoIndex) {
		/*
		 *  Loop through all edges
		 */

    // List of type FL_LinkMatchResult to store all edge information from subgraph
    List<FL_LinkMatchResult> links = new ArrayList<FL_LinkMatchResult>();
    for (int i = 0; i < uiucQueryEdgetoIndex.size(); i++)
      links.add(new FL_LinkMatchResult());


    for (String edgStr : uiucQueryEdgetoIndex.keySet()) {
			
			/*
			 * Process the edge to build FL_Link
			 */

      // Container to hold FL_Link (has other metadata involved)
      FL_LinkMatchResult linkMatchResult = new FL_LinkMatchResult();

      // Actual FL_Link
      FL_Link link = new FL_Link();
      link.setUid(edgStr);
      // Get parts of link
      String[] edgeSplit = edgStr.split("#");
      String src = edgeSplit[0];
      String dest = edgeSplit[1];
      double edge_weight = 1.0;

      link.setSource(src);
      link.setTarget(dest);
      //link.setTags(new ArrayList<FL_LinkTag>()); //Deprecated in Influent IDL 2.0

      link.setProperties(makeLinkProperties(src, dest));

      linkMatchResult.setScore(edge_weight);

      linkMatchResult.setUid("");
      linkMatchResult.setLink(link);

      links.add(uiucQueryEdgetoIndex.get(edgStr), linkMatchResult);
      links.remove(uiucQueryEdgetoIndex.get(edgStr) + 1);  //a bit hokey way to index, but FL_PatternSearchResult
      //expects an ArrayList.
    }
    //logger.info("links: "+links);
    //logger.info(queryEdges);
    //logger.info(executor.getActualQueryEdges());
    //logger.info("list: "+list);
    //logger.info("final nodes: "+nodes);


		/*
		 * Loop through all nodes
		 */

    // List of type FL_EntityMatchResult for entities involved in this sub-graph
    List<FL_EntityMatchResult> entities = new ArrayList<FL_EntityMatchResult>();

    for (int entityInd = 0; entityInd < exemplarIDs.size(); entityInd++) {
      FL_EntityMatchResult entityMatchResult = new FL_EntityMatchResult();

      entityMatchResult.setScore(1.0);
      entityMatchResult.setUid("E" + entityInd);

      FL_Entity entity = makeEntity(exemplarIDs.get(entityInd));

      // entity
      entityMatchResult.setEntity(entity);
      entities.add(entityMatchResult);
    }

		/*
		 * Set score,entities,links to result
		 */
    FL_PatternSearchResult result = new FL_PatternSearchResult();


    // infinity seems to break json parser
    result.setScore(Double.MAX_VALUE);//Double.POSITIVE_INFINITY);  //higher the score better
    result.setEntities(entities);
    result.setLinks(links);

    return result;
  }


  /**
   * @param queryNode2InfluentEntityInd
   * @param uiucQueryEdgetoIndex
   * @paramx exemplarIDs
   */
  private Map<SortedSet<String>, Integer> computeQueryEdgeList2InfluentEntityIndMap(
//      List<String> exemplarIDs,
      Map<String, Integer> queryNode2InfluentEntityInd,
      Map<String, Integer> uiucQueryEdgetoIndex) {
    int i;
		/*
		 * Map to later help map query node roles to result node roles ("E0," etc.)
		 */
    Map<Integer, SortedSet<String>> influentEntityInd2QueryEdgeList = new HashMap<>();
//		// initialize
//		for (i=0; i< exemplarIDs.size(); i++)
//			influentEntityInd2QueryEdgeList.put(i,new TreeSet<String>(Collator.getInstance()));

    for (String edgStr : uiucQueryEdgetoIndex.keySet()) {
      String edgId = "edg" + uiucQueryEdgetoIndex.get(edgStr);
      String[] edgNodes = edgStr.split("#");

      Integer influentInd;
      SortedSet<String> queryEdgeList;
      for (i = 0; i < 2; i++) {
        influentInd = queryNode2InfluentEntityInd.get(edgNodes[i]);
        if (!influentEntityInd2QueryEdgeList.containsKey(influentInd))
          influentEntityInd2QueryEdgeList.put(influentInd, new TreeSet<>(Collator.getInstance()));
        queryEdgeList = influentEntityInd2QueryEdgeList.get(influentInd);
        queryEdgeList.add(edgId);
        influentEntityInd2QueryEdgeList.put(influentInd, queryEdgeList);
      }
    }
    logger.info(influentEntityInd2QueryEdgeList);

    Map<SortedSet<String>, Integer> queryEdgeList2InfluentEntityInd = new HashMap<>();
    for (Integer influentEntityInd : influentEntityInd2QueryEdgeList.keySet()) {
      SortedSet<String> queryEdgeList = influentEntityInd2QueryEdgeList.get(influentEntityInd);
      queryEdgeList2InfluentEntityInd.put(queryEdgeList, influentEntityInd);
    }
    logger.info(queryEdgeList2InfluentEntityInd);

    return queryEdgeList2InfluentEntityInd;
  }


  /**
   * @paramx exemplarIDs
   * @paramx queryNode2InfluentEntityInd
   * @paramx uiucQueryEdgetoIndex
   */
  private Map<String, SortedSet<String>> computeResultNode2EdgeListMap(List<String> edges/*, int numNodes*/) {
    Map<String, SortedSet<String>> node2EdgeList = new HashMap<>();

    for (int i = 0; i < edges.size(); i++) {
      String edgId = "edg" + i;

      String[] edgeSplit = edges.get(i).split("#");
      String n1 = edgeSplit[0];
      String n2 = edgeSplit[1];

      if (!node2EdgeList.containsKey(n1))
        node2EdgeList.put(n1, new TreeSet<String>(Collator.getInstance()));
      SortedSet<String> edgeList = node2EdgeList.get(n1);
      edgeList.add(edgId);
      node2EdgeList.put(n1, edgeList);

      if (!node2EdgeList.containsKey(n2))
        node2EdgeList.put(n2, new TreeSet<String>(Collator.getInstance()));
      edgeList = node2EdgeList.get(n2);
      edgeList.add(edgId);
      node2EdgeList.put(n2, edgeList);
    }
    //logger.info(node2EdgeList);

    return node2EdgeList;
  }


  /**
   * Get nodes involved in subgraph from list of edges
   * <p>
   * TODO : why do we have to encode the nodes on an edge as a string like src#dst ???
   *
   * @param list
   * @paramx nodes
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
   * @param src
   * @param dest
   * @return
   */
  private List<FL_Property> getLinkProperties(String src, String dest) {
    List<FL_Property> linkProperties = new ArrayList<FL_Property>();
    if (SKIP_LINK_PROPERTIES) return linkProperties;

    //get edge attributes
    Map<String, String> edgeAttributes = binding.getEdgeAttributes(src, dest, edgeAttributeName2Type.keySet());
    Map<String, FL_PropertyType> h2Type2InfluentType = binding.getH2Type2InfluentType();

    for (String key : edgeAttributeName2Type.keySet()) {
      String keyType = edgeAttributeName2Type.get(key);
      FL_Property property = new FL_Property();

      String val = edgeAttributes.get(key);

      property.setKey(key);
      property.setFriendlyText(val);
			
			/*
			 * Cast to appropriate type
			 */
      if (keyType.equals("INTEGER")) {
        int castVal;
        if (val != null) {
          castVal = Integer.parseInt(val);
        } else {
          castVal = 0;
        }
        property.setRange(new FL_SingletonRange(castVal, h2Type2InfluentType.get(keyType)));
      } else if (keyType.equals("ID_COL_TYPE")) {
        long castVal;
        if (val != null) {
          castVal = Long.parseLong(val, 10); //assuming base10
        } else {
          castVal = 0;
        }
        property.setRange(new FL_SingletonRange(castVal, h2Type2InfluentType.get(keyType)));
      } else if (keyType.equals("DOUBLE") || keyType.equals("DECIMAL")) {
        double castVal;
        if (val != null) {
          castVal = Double.parseDouble(val);
        } else {
          castVal = 0.0;
        }
        property.setRange(new FL_SingletonRange(castVal, h2Type2InfluentType.get(keyType)));
      } else {
        //if "VARCHAR", "ARRAY", "OTHER" or anything else, keep as String
        String castVal;
        if (val != null) {
          castVal = val;
        } else {
          castVal = "";
        }
        property.setRange(new FL_SingletonRange(castVal, h2Type2InfluentType.get(keyType)));
      }

      property.setTags(new ArrayList<FL_PropertyTag>());

      //{TOT_OUT=DECIMAL, TOT_IN=DECIMAL, TOT_USD=DECIMAL, NUM_TRANS=ID_COL_TYPE}
      //binding.h2Type2InfluentType.get()

      linkProperties.add(property);
    }


    return linkProperties;
  }

  /**
   * @return
   * @see #loadTypesAndIndices()
   */
  private HashMap<String, String> getGraphEdgeAttributeName2Type() {

    HashMap<String, String> graphEdgeAttributeName2Type = new HashMap<String, String>();

    String sql = "select column_name, type_name from INFORMATION_SCHEMA.columns";
    sql += " where table_name = '" + graphTable + "' " +
        "and COLUMN_NAME <> '" + pairIDColumn + "' " +
        "and COLUMN_NAME <> '" + "SOURCE" + "' " +
        "and COLUMN_NAME <> '" + "TARGET" + "'"
    ;

    try {
      PreparedStatement queryStatement = binding.connection.prepareStatement(sql);
      //queryStatement.setString(1, graphTable);
      //queryStatement.setString(2, pairIDColumn);
      ResultSet rs = queryStatement.executeQuery();

      while (rs.next()) {
        graphEdgeAttributeName2Type.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
      }

      rs.close();
      queryStatement.close();

      Runtime.getRuntime().gc();

    } catch (SQLException e) {
      logger.info("Got e: " + e);
    }

    return graphEdgeAttributeName2Type;
  }


  /**
   * Get guid for subgraph defined by it's node set
   *
   * @param nodes
   * @return
   */
  private String getSubgraphGuid(Collection<String> nodes) {
    List<String> nodeList = new ArrayList<String>(nodes);

    // sort nodes
    Collections.sort(nodeList, new Comparator<String>() {
      @Override
      public int compare(String n1, String n2) {
        return binding.compareEntities(n1, n2);
      }
    });

    int count = 0;
    String subgraphGuid = "";
    for (String uid : nodeList) {
      if (count == 0) {
        subgraphGuid = uid;
      } else {
        subgraphGuid += "#" + uid;
      }
      count++;
    }

    return subgraphGuid;
  }

//		  @Override
//		  public int compareEntities(String e1, String e2) {
//			  if (Integer.parseInt(e1) < Integer.parseInt(e2)) {
//				  return -1;
//			  } else if (Integer.parseInt(e1) > Integer.parseInt(e2)){
//				  return 1;
//			  } else {
//				  return 0;
//			  }
//		  }


  /**
   * If we're on Windows, convert file-separators to "/"
   * (this should probably be in an utilities package at some point)
   */
  private static void windowsPathnamePortabilityCheck() {
    String osName = System.getProperty("os.name");

    if (osName.startsWith("Windows")) {
      String separator = Pattern.quote(System.getProperty("file.separator"));

      QueryExecutor.baseDir = QueryExecutor.baseDir.replace(separator, "/");
      QueryExecutor.graphFile = QueryExecutor.graphFile.replace(separator, "/");
      QueryExecutor.typesFile = QueryExecutor.typesFile.replace(separator, "/");
      QueryExecutor.queryFile = QueryExecutor.queryFile.replace(separator, "/");
      QueryExecutor.queryTypesFile = QueryExecutor.queryTypesFile.replace(separator, "/");
      // QueryExecutor.spathFile = QueryExecutor.spathFile.replace(separator, "/");
      QueryExecutor.topologyFile = QueryExecutor.topologyFile.replace(separator, "/");
      QueryExecutor.spdFile = QueryExecutor.spdFile.replace(separator, "/");
      QueryExecutor.resultDir = QueryExecutor.resultDir.replace(separator, "/");
    }
  }

}
