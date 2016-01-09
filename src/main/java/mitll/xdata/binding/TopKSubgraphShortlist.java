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
import mitll.xdata.db.DBConnection;
import org.apache.log4j.Logger;
import org.jgrapht.util.FibonacciHeap;
import org.jgrapht.util.FibonacciHeapNode;
import uiuc.topksubgraph.Edge;
import uiuc.topksubgraph.Graph;
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
  public static final String MARGINAL_GRAPH = "MARGINAL_GRAPH";

  private int K = 500;
  private int D = 2;

  private String usersTable = "users";
  private String userIdColumn = "user";
  private String typeColumn = "type";

  private String graphTable = "MARGINAL_GRAPH";
  private String pairIDColumn = "sorted_pairr";
  private HashMap<String, String> edgeAttributeName2Type;

  private final QueryExecutor executor;

  private static PreparedStatement queryStatement;

  /**
   * @param binding
   * @see mitll.xdata.dataset.bitcoin.binding.BitcoinBinding#BitcoinBinding(DBConnection, boolean, String)
   */
  public TopKSubgraphShortlist(Binding binding) {
    super(binding);

    //setup shortlist stuff here having to do with state...
    logger.info("setting up query executor... this should only happen once...");
    executor = new QueryExecutor();

    QueryExecutor.datasetId = binding.datasetId;
    QueryExecutor.baseDir = "src/main/resources" + binding.datasetResourceDir; //THIS LINE SHOULD CHANGE FOR JAR-ed VERSION
    //QueryExecutor.k0 = D;
    //QueryExecutor.topK= K;

    //load graph (this may just need to be rolled up into BitcoinBinding)
    logger.info("Loading graph. This should only happen once...");
    Graph g = new Graph();
    try {
      //  g.loadGraph(binding.connection, "MARGINAL_GRAPH", "NUM_TRANS");
      g.loadGraphAgain(binding.connection, MARGINAL_GRAPH, "NUM_TRANS");
    } catch (Exception e) {
      logger.info("Got: " + e);
    }
    executor.g = g;

    QueryExecutor.spathFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".spath";
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
   */
  public void loadTypesAndIndices() {
    /*
     * Load-in types, and count how many there are
		 */
    try {
      logger.info("Loading in types...");
      executor.loadTypesFromDatabase(binding.connection, usersTable, userIdColumn, typeColumn);

      logger.info("Loading in indices...");
      executor.loadGraphNodesType();    //compute ordering
      executor.loadGraphSignatures();    //topology
      executor.loadEdgeLists();      //sorted edge lists
      executor.loadSPDIndex();      //spd index


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
   * @see Binding#getShortlist(FL_PatternDescriptor, long)
   */
  @Override
  public List<FL_PatternSearchResult> getShortlist(List<FL_EntityMatchDescriptor> entityMatchDescriptorsIgnored,
                                                   List<String> exemplarIDs, long max) {
    // which binding are we bound to?
    logger.info(this.binding.toString());
    logger.info(this.binding.connection);


    //check to see if we can connect to anything....
    if (existsTable(graphTable)) {
      logger.info("we can connect to the right database...");
    } else {
      logger.info("table " + graphTable + " does not yet exist.");
    }


		/*
     * Get all pairs of query nodes...
		 * (this is assuming ids are sortable by integer comparison, like in bitcoin)
		 */
    HashSet<Edge> queryEdges = new HashSet<Edge>();
    Edge edg;
    int e1, e2;
    String pair;

    logger.info("ran on " + exemplarIDs);

    if (exemplarIDs.size() > 1) {
      for (int i = 0; i < exemplarIDs.size(); i++) {
        for (int j = i + 1; j < exemplarIDs.size(); j++) {
          String e1ID = exemplarIDs.get(i);
          e1 = Integer.parseInt(e1ID);
          String e2ID = exemplarIDs.get(j);
          e2 = Integer.parseInt(e2ID);
          if (Integer.parseInt(e1ID) <= Integer.parseInt(e2ID)) {
            pair = "(" + e1 + "," + e2 + ")";
            edg = new Edge(e1, e2, 1.0);  //put in here something to get weight if wanted...
          } else {
            pair = "(" + e2 + "," + e1 + ")";
            edg = new Edge(e2, e1, 1.0);  //put in here something to get weight if wanted...
          }

          // if (existsPair(graphTable, pairIDColumn, pair)) {
          if (existsPairTransactions(BitcoinBinding.TRANSACTIONS, e1ID, e2ID)) {
            queryEdges.add(edg);
          } else {
            logger.warn("no edge between " + e1ID + " and " + e2ID);
          }
        }
      }
    }

    for (Edge qe : queryEdges) {
      logger.info("qe: " + qe);
    }
    int isClique = executor.loadQuery(queryEdges, binding.connection, usersTable, userIdColumn, typeColumn);
    logger.info("isClique: " + isClique);

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

    /**
     * Get query signatures
     */
    executor.getQuerySignatures(); //fills in querySign

    /**
     * NS Containment Check and Candidate Generation
     */
    long time1 = new Date().getTime();

    int prunedCandidateFiltering = executor.generateCandidates();
    //if (prunedCandidateFiltering < 0) {
    //	return;
    //}

    long timeA = new Date().getTime();
    System.out.println("Candidate Generation Time: " + (timeA - time1));


    /**
     * Populate all required HashMaps relating edges to edge-types
     */
    // compute edge types for all edges in query
    HashSet<String> queryEdgeTypes = executor.computeQueryEdgeTypes();

    //compute queryEdgetoIndex
    executor.computeQueryEdge2Index();

    //compute queryEdgeType2Edges
    HashMap<String, ArrayList<String>> queryEdgeType2Edges = executor.computeQueryEdgeType2Edges();


    //Maintain pointers and topk heap
    executor.computePointers(queryEdgeTypes, queryEdgeType2Edges);

    /**
     * The secret sauce... Execute the query...
     */
    executor.executeQuery(queryEdgeType2Edges, isClique, prunedCandidateFiltering);

    long time2 = new Date().getTime();
    System.out.println("Overall Time: " + (time2 - time1));


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

    logger.info("Original entity ordering from Influent query: " + exemplarIDs);
    List<FL_PatternSearchResult> results = getPatternSearchResults(queryEdges, exemplarIDs);

    return results;
  }


  /**
   * @param connection
   * @param tableName
   */
  private boolean existsTable(String tableName) {
    try {
      String sql = "select 1 from " + tableName + " limit 1;";
      ResultSet rs = doSQLQuery(sql);
      rs.close();
      queryStatement.close();

      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  private boolean existsPairTransactions(String tableName, String source, String target) {
    try {
      String sql = "select count(*) as CNT from " + tableName +
          " where " + "(" +
          "SOURCE" + " = " + source + " AND " +
          "TARGET" + " = " + target + ")" +
          " OR " +
          "(" +
          "TARGET" + " = " + source + " AND " +
          "SOURCE" + " = " + target + ")" +
          " limit 1;";

      ResultSet rs = doSQLQuery(sql);
      rs.next();
      int cnt = rs.getInt("CNT");
      rs.close();
      queryStatement.close();

      return cnt > 0;
    } catch (SQLException e) {
      logger.info("got e: " + e);
      return false;
    }
  }

  /**
   * @param tableName
   * @param pairID
   * @return
   */
  private boolean existsPair(String tableName, String pairIdCol, String pair) {
    try {
      String sql = "select count(*) as CNT from " + tableName + " where " + pairIdCol + " = " + pair + " limit 1;";
      ResultSet rs = doSQLQuery(sql);
      rs.next();
      int cnt = rs.getInt("CNT");
      rs.close();
      queryStatement.close();

      if (cnt > 0) {
        return true;
      } else {
        return false;
      }
    } catch (SQLException e) {
      logger.info("got e: " + e);
      return false;
    }
  }


  /**
   * Do SQL, query something, return that something
   *
   * @param createSQL
   * @return
   * @throws SQLException
   */
  private ResultSet doSQLQuery(String sql) throws SQLException {
    //queryStatement = connection.prepareStatement(sql);
    queryStatement = this.binding.connection.prepareStatement(sql);
    ResultSet rs = queryStatement.executeQuery();

    return rs;
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
   */
  private List<FL_PatternSearchResult> getPatternSearchResults(HashSet<Edge> queryEdges, List<String> exemplarIDs) {

    // Heap of results from uiuc.topksubgraph
    FibonacciHeap<ArrayList<String>> heap = executor.getHeap();

    // List of type FL_PatternSearchResult to hold all matching sub-graph contained in heap
    List<FL_PatternSearchResult> results = new ArrayList<FL_PatternSearchResult>();

		/*
		 * Map query node names to their corresponding entity ind from the Influent JSON 
		 */
    HashMap<String, Integer> queryNode2InfluentEntityInd = new HashMap<String, Integer>();
    int i = 0;
    for (String node : exemplarIDs) {
      queryNode2InfluentEntityInd.put(node, i);
      i++;
    }
		
		/*
		 * Map between influent and uiuc.topksubraph index structure for query edges 
		 */
    HashMap<String, Integer> uiucQueryEdgetoIndex = executor.getQueryEdgetoIndex();
    HashMap<Integer, Integer> uiucInd2InfluentInd = new HashMap<Integer, Integer>();

    i = 0;
    for (Edge qe : queryEdges) {
      //assuming here qe.src and qe.dst are ordered properly (i.e. src < dst)
      String edgeString = qe.src + "#" + qe.dst;
      int uiucInd = uiucQueryEdgetoIndex.get(edgeString);
      uiucInd2InfluentInd.put(uiucInd, i);
      i++;
    }

    // Compute map between influent entity ind and list of edges
    HashMap<TreeSet<String>, Integer> queryEdgeList2InfluentEntityInd = computeQueryEdgeList2InfluentEntityIndMap(exemplarIDs,
        queryNode2InfluentEntityInd, uiucQueryEdgetoIndex);
		
		/*
		 * Convert query to FL_PatternSearchResult and add to results...
		 */
    FL_PatternSearchResult queryAsResult = makeQueryIntoResult(exemplarIDs, uiucQueryEdgetoIndex);
    results.add(queryAsResult);


    // Set of unique subgraphs found
    HashSet<String> uniqueSubgraphGuids = new HashSet<String>();

    logger.info("Starting with: " + heap.size() + " matching sub-graphs...");

    // Loop-through resultant sub-graphs
    while (!heap.isEmpty()) {
      // List of type FL_EntityMatchResult for entities involved in this sub-graph
      List<FL_EntityMatchResult> entities = new ArrayList<FL_EntityMatchResult>();

      // Get matching sub-graph
      FibonacciHeapNode<ArrayList<String>> fhn = heap.removeMin();
      ArrayList<String> list = fhn.getData();

      // Sub-graph score
      double subgraphScore = fhn.getKey();

      // HashSet for entities involved in this sub-graph
      HashSet<String> nodes = getSubgraphNodes(list);

      // make subgraph guid
      String subgraphGuid = getSubgraphGuid(nodes);

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
        ArrayList<FL_LinkMatchResult> links = new ArrayList<FL_LinkMatchResult>();
        for (i = 0; i < list.size(); i++)
          links.add(new FL_LinkMatchResult());


        for (i = 0; i < list.size(); i++) {
					/*
					 * Process the edge to build FL_Link
					 */

          // Container to hold FL_Link (has other metadata involved)
          FL_LinkMatchResult linkMatchResult = new FL_LinkMatchResult();

          // Actual FL_Link
          FL_Link link = new FL_Link();

          // Get parts of link
          String[] edgeSplit = list.get(i).split("#");
          String src = edgeSplit[0];
          String dest = edgeSplit[1];
          double edge_weight = Double.parseDouble(edgeSplit[2]);

          link.setSource(src);
          link.setTarget(dest);
          //link.setTags(new ArrayList<FL_LinkTag>()); //Deprecated in Influent IDL 2.0

          List<FL_Property> linkProperties;
          if (binding.compareEntities(src, dest) == 1) {
            linkProperties = getLinkProperties(dest, src);
          } else {
            linkProperties = getLinkProperties(src, dest);
          }
          link.setProperties(linkProperties);

          linkMatchResult.setScore(edge_weight);

          linkMatchResult.setUid("");
          linkMatchResult.setLink(link);
          //links.add(linkMatchResult);

					/*
					links.add(uiucInd2InfluentInd.get(i),linkMatchResult);
					links.remove(uiucInd2InfluentInd.get(i)+1); //a bit hokey...
					*/
          links.add(i, linkMatchResult);
          links.remove(i + 1); //a bit hokey way to index, but FL_PatternSearchResult
          //expects an ArrayList.

					/*
					 * Track nodes (as currently written doesn't get into these conditionals..)
					 */
          if (!nodes.contains(src)) {
            nodes.add(src);
          }
          if (!nodes.contains(dest)) {
            nodes.add(dest);
          }

        }
        //logger.info("links: "+links);
        //logger.info(queryEdges);
        //logger.info(executor.getActualQueryEdges());
        //logger.info("list: "+list);
        //logger.info("final nodes: "+nodes);
				
				
				/*
				 * Get mapping between nodes and all edges they are involved in (for node correspondence mapping b/w query and result)
				 */
        HashMap<String, TreeSet<String>> resultNode2EdgeList = computeResultNode2EdgeListMap(list, nodes.size());
				

				/*
				 * Loop through all nodes
				 */
        Iterator<String> iter = nodes.iterator();
        while (iter.hasNext()) {
          String node = iter.next();

          FL_EntityMatchResult entityMatchResult = new FL_EntityMatchResult();

          int entityInd = queryEdgeList2InfluentEntityInd.get(resultNode2EdgeList.get(node));
          entityMatchResult.setScore(1.0);
          entityMatchResult.setUid("E" + entityInd);

          //= binding.makeEntityMatchResult(exemplarQueryID, similarID, similarity);
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

          // entity
          entityMatchResult.setEntity(entity);
          if (entityMatchResult != null) {
            entities.add(entityMatchResult);
          }
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
      }
    }

    logger.info("Ending with: " + uniqueSubgraphGuids.size() + " unique matching sub-graphs...");

    return results;
  }


  /**
   * Convert query graphs into pattern search "result".
   *
   * @param queryEdges
   * @return
   */
  private FL_PatternSearchResult makeQueryIntoResult(List<String> exemplarIDs, HashMap<String, Integer> uiucQueryEdgetoIndex) {

		/*
		 *  Loop through all edges
		 */

    // List of type FL_LinkMatchResult to store all edge information from subgraph
    ArrayList<FL_LinkMatchResult> links = new ArrayList<FL_LinkMatchResult>();
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

      // Get parts of link
      String[] edgeSplit = edgStr.split("#");
      String src = edgeSplit[0];
      String dest = edgeSplit[1];
      double edge_weight = 1.0;

      link.setSource(src);
      link.setTarget(dest);
      //link.setTags(new ArrayList<FL_LinkTag>()); //Deprecated in Influent IDL 2.0

      List<FL_Property> linkProperties;
      if (binding.compareEntities(src, dest) == 1) {
        linkProperties = getLinkProperties(dest, src);
      } else {
        linkProperties = getLinkProperties(src, dest);
      }
      link.setProperties(linkProperties);

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

      FL_Entity entity = new FL_Entity();
      entity.setUid(exemplarIDs.get(entityInd));
      entity.setTags(new ArrayList<FL_EntityTag>());

      List<FL_Property> entityProperties = new ArrayList<FL_Property>();

      // node_id
      FL_Property property = new FL_Property();
      property.setKey("node_id");
      property.setFriendlyText(exemplarIDs.get(entityInd));
      property.setRange(new FL_SingletonRange(exemplarIDs.get(entityInd), FL_PropertyType.STRING));
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

      // entity
      entityMatchResult.setEntity(entity);
      if (entityMatchResult != null) {
        entities.add(entityMatchResult);
      }
    }

		/*
		 * Set score,entities,links to result
		 */
    FL_PatternSearchResult result = new FL_PatternSearchResult();

    result.setScore(Double.POSITIVE_INFINITY);  //higher the score better
    result.setEntities(entities);
    result.setLinks(links);

    return result;
  }


  /**
   * @param exemplarIDs
   * @param queryNode2InfluentEntityInd
   * @param uiucQueryEdgetoIndex
   */
  private HashMap<TreeSet<String>, Integer> computeQueryEdgeList2InfluentEntityIndMap(
      List<String> exemplarIDs,
      HashMap<String, Integer> queryNode2InfluentEntityInd,
      HashMap<String, Integer> uiucQueryEdgetoIndex) {
    int i;
		/*
		 * Map to later help map query node roles to result node roles ("E0," etc.)
		 */
    HashMap<Integer, TreeSet<String>> influentEntityInd2QueryEdgeList = new HashMap<Integer, TreeSet<String>>();
//		// initialize
//		for (i=0; i< exemplarIDs.size(); i++)
//			influentEntityInd2QueryEdgeList.put(i,new TreeSet<String>(Collator.getInstance()));

    for (String edgStr : uiucQueryEdgetoIndex.keySet()) {
      String edgId = "edg" + uiucQueryEdgetoIndex.get(edgStr);
      String[] edgNodes = edgStr.split("#");

      Integer influentInd;
      TreeSet<String> queryEdgeList;
      for (i = 0; i < 2; i++) {
        influentInd = queryNode2InfluentEntityInd.get(edgNodes[i]);
        if (!influentEntityInd2QueryEdgeList.containsKey(influentInd))
          influentEntityInd2QueryEdgeList.put(influentInd, new TreeSet<String>(Collator.getInstance()));
        queryEdgeList = influentEntityInd2QueryEdgeList.get(influentInd);
        queryEdgeList.add(edgId);
        influentEntityInd2QueryEdgeList.put(influentInd, queryEdgeList);
      }
    }
    logger.info(influentEntityInd2QueryEdgeList);

    HashMap<TreeSet<String>, Integer> queryEdgeList2InfluentEntityInd = new HashMap<TreeSet<String>, Integer>();
    for (Integer influentEntityInd : influentEntityInd2QueryEdgeList.keySet()) {
      TreeSet<String> queryEdgeList = influentEntityInd2QueryEdgeList.get(influentEntityInd);
      queryEdgeList2InfluentEntityInd.put(queryEdgeList, influentEntityInd);
    }
    logger.info(queryEdgeList2InfluentEntityInd);

    return queryEdgeList2InfluentEntityInd;
  }


  /**
   * @param exemplarIDs
   * @param queryNode2InfluentEntityInd
   * @param uiucQueryEdgetoIndex
   */
  private HashMap<String, TreeSet<String>> computeResultNode2EdgeListMap(ArrayList<String> edges, int numNodes) {

    HashMap<String, TreeSet<String>> node2EdgeList = new HashMap<String, TreeSet<String>>();

    for (int i = 0; i < edges.size(); i++) {
      String edgId = "edg" + i;

      String[] edgeSplit = edges.get(i).split("#");
      String n1 = edgeSplit[0];
      String n2 = edgeSplit[1];

      if (!node2EdgeList.containsKey(n1))
        node2EdgeList.put(n1, new TreeSet<String>(Collator.getInstance()));
      TreeSet<String> edgeList = node2EdgeList.get(n1);
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
   *
   * @param nodes
   * @param list
   */
  private HashSet<String> getSubgraphNodes(ArrayList<String> list) {
    HashSet<String> nodes = new HashSet<String>();

    for (int i = 0; i < list.size(); i++) {
      // Get parts of edge
      String[] edgeSplit = list.get(i).split("#");
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


  private List<FL_Property> getLinkProperties(String src, String dest) {

    List<FL_Property> linkProperties = new ArrayList<FL_Property>();

    //get edge attributes
    HashMap<String, String> edgeAttributes = binding.getEdgeAttributes(src, dest, edgeAttributeName2Type.keySet());
    HashMap<String, FL_PropertyType> h2Type2InfluentType = binding.getH2Type2InfluentType();

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
      } else if (keyType.equals("BIGINT")) {
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

      //{TOT_OUT=DECIMAL, TOT_IN=DECIMAL, TOT_USD=DECIMAL, NUM_TRANS=BIGINT}
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
      queryStatement = binding.connection.prepareStatement(sql);
      //queryStatement.setString(1, graphTable);
      //queryStatement.setString(2, pairIDColumn);
      ResultSet rs = queryStatement.executeQuery();

      while (rs.next()) {
        graphEdgeAttributeName2Type.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
      }

      rs.close();
      queryStatement.close();

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
  private String getSubgraphGuid(HashSet<String> nodes) {

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
      QueryExecutor.spathFile = QueryExecutor.spathFile.replace(separator, "/");
      QueryExecutor.topologyFile = QueryExecutor.topologyFile.replace(separator, "/");
      QueryExecutor.spdFile = QueryExecutor.spdFile.replace(separator, "/");
      QueryExecutor.resultDir = QueryExecutor.resultDir.replace(separator, "/");
    }
  }

}
