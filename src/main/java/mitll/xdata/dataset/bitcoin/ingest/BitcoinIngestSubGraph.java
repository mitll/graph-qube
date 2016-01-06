// Copyright 2015 MIT Lincoln Laboratory, Massachusetts Institute of Technology 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mitll.xdata.dataset.bitcoin.ingest;

import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesBase;
import mitll.xdata.dataset.bitcoin.features.FeaturesSql;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import org.apache.log4j.Logger;
import uiuc.topksubgraph.Graph;
import uiuc.topksubgraph.MultipleIndexConstructor;
import uiuc.topksubgraph.QueryExecutor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;


/**
 * Bitcoin Ingest Class: SubGraph Search
 * <p>
 * Pre-processing the graph to prepare it for topk-subgraph ingest/indexing:
 * - Filter-out non-active nodes, self-transitions, heavy-hitters
 * - Create marginalized graph data and various stats
 * - Do the indexing for the topk-subgraph algorithm
 *
 * @author Charlie Dagli, dagli@ll.mit.edu
 */
public class BitcoinIngestSubGraph {

  private static final Logger logger = Logger.getLogger(BitcoinIngestSubGraph.class);

  private static final int MIN_TRANSACTIONS = 10;

  //private static final int BITCOIN_OUTLIER = 25;
  private static final List<Integer> BITCOIN_OUTLIERS = Arrays.asList(25, 39);

  private static final String TABLE_NAME = "transactions";
  private static final String MARGINAL_GRAPH = "MARGINAL_GRAPH";
  private static final String GRAPH_TABLE = MARGINAL_GRAPH;

  private static final String USERS = "users";
  private static final String TYPE_COLUMN = "type";
  private static final String USERID_COLUMN = "user";
  public static final String SORTED_PAIR = "SORTED_PAIR";


  private static PreparedStatement queryStatement;
  private static final String bitcoinDirectory = "src/main/resources" +
      BitcoinBinding.BITCOIN_FEATS_TSV;


  /**
   * @param dbConnection
   * @param tableName
   */
  private static boolean existsTable(DBConnection dbConnection, String tableName) {
    try {
      ResultSet rs = doSQLQuery(dbConnection, "select 1 from " + tableName + " limit 1;");
      rs.close();
      return true;
    } catch (SQLException e) {
      return false;
    }
  }


  /**
   * @throws FileNotFoundException
   * @throws IOException
   * @throws Throwable
   * @throws NumberFormatException
   */
  public static void executeQuery(String outDir, DBConnection connection) throws
      Throwable {

	    /*
       * Setup QueryExecutor object
	     */
    QueryExecutor executor = new QueryExecutor();

    QueryExecutor.datasetId = BitcoinBinding.DATASET_ID;
    executor.g = MultipleIndexConstructor.getGraph(); //this is assuming graph has already been loaded..
    QueryExecutor.baseDir = outDir;
    QueryExecutor.k0 = 2;
    QueryExecutor.topK = 5001;

    QueryExecutor.spathFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".spath";
    QueryExecutor.topologyFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".topology";
    QueryExecutor.spdFile = QueryExecutor.datasetId + "." + QueryExecutor.k0 + ".spd";
    QueryExecutor.resultDir = "results";

    windowsPathnamePortabilityCheck();


    //String pattern = Pattern.quote(System.getProperty("file.separator"));
    //String pattern = "/";
    //String[] splitGraphFile = QueryExecutor.graphFile.split(pattern);
    //QueryExecutor.graphFileBasename = splitGraphFile[splitGraphFile.length-1];

		
		/*
     * Load-in types, and count how many there are
		 */
    executor.loadTypesFromDatabase(connection, USERS, USERID_COLUMN, TYPE_COLUMN);
    logger.info("Loaded-in types...");

    executor.loadGraphNodesType();    //compute ordering
    executor.loadGraphSignatures();    //topology
    executor.loadEdgeLists();      //sorted edge lists
    executor.loadSPDIndex();      //spd index

    //Make resultDir if necessary
    File directory = new File(QueryExecutor.baseDir + QueryExecutor.resultDir);
    if (!directory.exists() && !directory.mkdirs())
      throw new IOException("Could not create directory: " + QueryExecutor.baseDir + QueryExecutor.resultDir);

		
		/*
     * Execute queries
		 */
    String[] queries = new String[]{"FanOut", "FOFI"};
    for (int i = 0; i < queries.length; i++) {
			
			/*
			 * Setup and read-in query
			 */
      QueryExecutor.queryFile = "queries/queryGraph." + queries[i] + ".txt";
      QueryExecutor.queryTypesFile = "queries/queryTypes." + queries[i] + ".txt";

      //set system out to out-file...
      System.setOut(new PrintStream(new File(QueryExecutor.baseDir + QueryExecutor.resultDir +
          "/QueryExecutor.topK=" + QueryExecutor.topK +
          "_K0=" + QueryExecutor.k0 +
          "_" + QueryExecutor.datasetId +
          "_" + QueryExecutor.queryFile.split("/")[1])));


      int isClique = executor.loadQuery();

      /**
       * Get query signatures
       */
      executor.getQuerySignatures(); //fills in querySign

      /**
       * NS Containment Check and Candidate Generation
       */
      long time1 = new Date().getTime();

      int prunedCandidateFiltering = executor.generateCandidates();
      if (prunedCandidateFiltering < 0) {
        return;
      }

      long timeA = new Date().getTime();
      System.out.println("Candidate Generation Time: " + (timeA - time1));


			/*
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

			/*
			 * The secret sauce... Execute the query...
			 */
      executor.executeQuery(queryEdgeType2Edges, isClique, prunedCandidateFiltering);

      long time2 = new Date().getTime();
      System.out.println("Overall Time: " + (time2 - time1));


      //FibonacciHeap<ArrayList<String>> queryResults = executor.getHeap();
      executor.printHeap();
    }
  }


  /**
   * Overloaded {@link #computeIndices(String, DBConnection)}
   *
   * @param dbType
   * @param h2DatabaseName
   * @throws Throwable
   * @throws Exception
   * @see BitcoinIngestBase#doSubgraphs(String)
   */
  protected static void computeIndices(String dbType, String h2DatabaseName) throws Throwable {
    DBConnection connection = new IngestSql().getDbConnection(dbType, h2DatabaseName);

    long then = System.currentTimeMillis();
    logger.info("computeIndices start");

    computeIndices(bitcoinDirectory, connection);
    long now = System.currentTimeMillis();

    logger.info("computeIndices end took " + (now - then) + " millis");

    connection.closeConnection();
  }

  protected static void computeIndicesFromMemory(String dbType, String h2DatabaseName,
                                                 Map<Long, Integer> edgeToWeight) throws Throwable {
    DBConnection connection = new IngestSql().getDbConnection(dbType, h2DatabaseName);

    long then = System.currentTimeMillis();
    logger.info("computeIndices start");

    computeIndicesFromMemory(bitcoinDirectory, connection, edgeToWeight);
    long now = System.currentTimeMillis();

    logger.info("computeIndices end took " + (now - then) + " millis");

    connection.closeConnection();
  }

  private static void computeIndicesFromMemory(String bitcoinDirectory,
                                               DBConnection dbConnection,
                                               Map<Long, Integer> edgeToWeight) throws Throwable {
    // Load graph into topk-subgraph Graph object
    Graph g = new Graph();
    g.loadGraphFromMemory(edgeToWeight);
    computeIndices(bitcoinDirectory, dbConnection, g);
  }

  /**
   * Compute all indices needed for UIUC Top-K subgraph search algorithm
   *
   * @param bitcoinDirectory
   * @param dbConnection
   * @throws Throwable
   * @throws Exception
   * @throws IOException
   * @see #computeIndices(String, String)
   */
  private static void computeIndices(String bitcoinDirectory,
                                     DBConnection dbConnection) throws Throwable {
    // Load graph into topk-subgraph Graph object
    Graph g = new Graph();
    g.loadGraph(dbConnection, MARGINAL_GRAPH, "NUM_TRANS");
    computeIndices(bitcoinDirectory, dbConnection, g);
  }

  private static void computeIndices(String bitcoinDirectory, DBConnection dbConnection, Graph g) throws Exception {
  /*
   * This is stuff is doing the actual topk-subgraph indexing
   */
    MultipleIndexConstructor.outDir = bitcoinDirectory;
    MultipleIndexConstructor.D = BitcoinBinding.SHORTLISTING_D;


    MultipleIndexConstructor.setGraph(g);
    logger.info("Loaded graph from database...");

//	    //iterate through hashmap test
//	    int c=0;
//	    int max_val = 0;
//	    for (Integer key : g.node2NodeIdMap.keySet()) {
//	        if (g.node2NodeIdMap.get(key) > max_val) {
//	        	max_val = g.node2NodeIdMap.get(key);
//	        }
//	        c++;
//	    }
//	    logger.info("There are: "+c+" unique nodes in the graph...");
//	    logger.info("max internal-id val is..."+max_val);

//		//Insert default type into users table... (this is actually now done in BitcoinFeatures)
//		String sqlInsertType = "alter table "+USERS+" drop if exists "+TYPE_COLUMN+";"+
//								" alter table "+USERS+" add "+TYPE_COLUMN+" int not null default(1);";
//		doSQLUpdate(dbConnection, sqlInsertType);


    new FeaturesSql().createUsersTableNoDrop(dbConnection.getConnection());
    // Load types into topk-subgraph object...
    MultipleIndexConstructor.loadTypesFromDatabase(dbConnection, USERS, USERID_COLUMN, TYPE_COLUMN);
    logger.info("Loaded types from database...");
    logger.debug("Number of types: " + MultipleIndexConstructor.totalTypes);

    // Create Typed Edges
    MultipleIndexConstructor.createTypedEdges();

    // Load and Sort Edges from Graph
    //MultipleIndexConstructor.loadAndSortEdges();
    MultipleIndexConstructor.populateSortedEdgeLists();

    //save the sorted edge lists
    MultipleIndexConstructor.saveSortedEdgeList();

    //hash map for all possible "edge-type" paths: i.e. doubles,triples,...D-tuples
    //this gets you the "official" ordering
    logger.info("Computing Edge-Type Path Ordering...");
    MultipleIndexConstructor.computeEdgeTypePathOrdering();

    logger.info("Computing SPD, Topology and SPath Indices...");
    MultipleIndexConstructor.computeIndices();
  }


  /**
   * Find all unique edges and gather statistics about the activity between them.
   * <p>
   * The primary key in this table is the tuple "sorted_pair" which is a guid for each edge
   * where the accounts involved in a transaction are listed in numerical order;
   * e.g. (a,b) for all transactions between a and b, given a < b.
   * <p>
   * The column "TOT_USD" sums the total amount transacted between a and b
   * The column "NUM_TRANS" count the total number of transactions between a and b
   * The column "TOT_OUT" sums the value of transactions from a->b
   * The column "TOT_IN" sums the value of transactions from b->a
   *
   * @param dbType
   * @return
   * @throws Exception
   * @see BitcoinIngestBase#doSubgraphs(String)
   */
  protected static Map<Long, Integer> extractUndirectedGraph(String dbType, String h2DatabaseName) throws Exception {
    DBConnection connection = new IngestSql().getDbConnection(dbType, h2DatabaseName);

    long then = System.currentTimeMillis();
    logger.info("extractUndirectedGraph start");
    Map<Long, Integer> edgeToWeight = extractUndirectedGraphInMemory(connection);
    //  extractUndirectedGraph(connection);
    long now = System.currentTimeMillis();

    logger.info("extractUndirectedGraph end took " + (now - then) + " millis");

    connection.closeConnection();
    return edgeToWeight;//Collections.emptyMap();
  }

  /**
   * Undirected graph - count transactions on each edge.
   *
   * @param connection
   * @return
   * @throws Exception
   */
  private static Map<Long, Integer> extractUndirectedGraphInMemory(DBConnection connection) throws Exception {
    // Map<Integer, Map<Integer, Integer>> sourceToDestToValue = new HashMap<>();

    ResultSet rs = doSQLQuery(connection,
        "select " + IngestSql.SOURCE + "," + IngestSql.TARGET + " from " + TABLE_NAME
    );
    Map<Long, Integer> sourceToDest = new HashMap<>();

    int c = 0;
    while (rs.next()) {
      int source = rs.getInt(1);
      int target = rs.getInt(2);

//      if (c < 20) logger.info("extractUndirectedGraphInMemory " +source + " -> " + target);

      if (source > target) {
        int tmp = source;
        source = target;
        target = tmp;
       // if (c < 2000) logger.info("\tflip " + source + " -> " + target);
      }
//      source = source < target ? source : target;
//      target = source < target ? target : source;


      long key = BitcoinFeaturesBase.storeTwo(source, target);

//      if (c < 20) {
//        logger.info("extractUndirectedGraphInMemory " +key + " = " + BitcoinFeaturesBase.getLow(key) + " -> " +BitcoinFeaturesBase.getHigh(key));
//      }

      Integer orDefault = sourceToDest.getOrDefault(key, 0);
      sourceToDest.put(key, orDefault + 1);

      c++;
    }

    logger.info("map is " + sourceToDest.size());
    rs.close();

    return sourceToDest;
  }

  /**
   * Overloaded {@link #extractUndirectedGraph(String, String)}
   * <p>
   * Find all unique edges and gather statistics about the activity between them.
   * <p>
   * The primary key in this table is the tuple "sorted_pair" which is a guid for each edge
   * where the accounts involved in a transaction are listed in numerical order;
   * e.g. (a,b) for all transactions between a and b, given a < b.
   * <p>
   * The column "TOT_USD" sums the total amount transacted between a and b
   * The column "NUM_TRANS" count the total number of transactions between a and b
   * The column "TOT_OUT" sums the value of transactions from a->b
   * The column "TOT_IN" sums the value of transactions from b->a
   *
   * @param connection
   * @return
   * @throws Exception
   * @see #extractUndirectedGraph(String, String)
   */
  private static void extractUndirectedGraph(DBConnection connection) throws Exception {
		
		/*
		 * Setup SQL statements
		 */
    String sqlGenerateSortedPairs =
/*
        "alter table " + TABLE_NAME + " drop column if exists " +
        SORTED_PAIR +
        ";" +

        " alter table " + TABLE_NAME + " add " +
        SORTED_PAIR +
        " array;" +
*/
        " update " + TABLE_NAME +
            " set sorted_pair =" +
            " case when (source > target)" +
            " then (target,source) " +
            "else (source,target) end;";

    String sqlPopulateMarginalGraph =
        "drop table if exists " + GRAPH_TABLE + ";" +

            " create table " + GRAPH_TABLE + " as " +
            " select sorted_pair, sum(usd) as TOT_USD, count(*) as NUM_TRANS" +
            " from " + TABLE_NAME + " group by sorted_pair;" +

            " alter table " + GRAPH_TABLE + " add TOT_OUT decimal(20,8);" +

            " alter table " + GRAPH_TABLE + " add TOT_IN decimal(20,8);";

    String sqlTotOutTemp = "drop table if exists tmp;" +
        " create temporary table tmp as select sorted_pair, sum(usd) as tot_out" +
        " from " + TABLE_NAME +
        " where (source < target)" +
        " group by sorted_pair;";

    String sqlUpdateTotOut = "update " + GRAPH_TABLE + " set tot_out =" +
        " (select tmp.tot_out" +
        " from tmp" +
        " where " + GRAPH_TABLE + ".sorted_pair = tmp.sorted_pair);";

    String sqlTotInTemp = "drop table if exists tmp;" +
        " create temporary table tmp as select sorted_pair, sum(usd) as tot_in" +
        " from " + TABLE_NAME +
        " where (source > target)" +
        " group by sorted_pair;";

    String sqlUpdateTotIn = "update " + GRAPH_TABLE + " set tot_in =" +
        " (select tmp.tot_in" +
        " from tmp" +
        " where " + GRAPH_TABLE + ".sorted_pair = tmp.sorted_pair);";

    String sqlDropTemp = "drop table tmp";

    logger.info("generate sorted pairs ");
    int i = doSQLUpdate(connection, sqlGenerateSortedPairs);
    logger.info("populate marginal graph " + i);
    logger.info("populate marginal graph sql " + sqlPopulateMarginalGraph);

    i = doSQLUpdate(connection, sqlPopulateMarginalGraph);

    logger.info("create sorted pair " + i);

    doSQLUpdate(connection, sqlTotOutTemp);

    logger.info("update marginal graph");

    doSQLUpdate(connection, sqlUpdateTotOut);

    logger.info("create total in degree");

    doSQLUpdate(connection, sqlTotInTemp);

    logger.info("update marginal graph 2");

    doSQLUpdate(connection, sqlUpdateTotIn);

    logger.info("update marginal graph 2 complete");

    doSQLUpdate(connection, sqlDropTemp);
  }


  /**
   * Filter out accounts involved in fewer than {@link #MIN_TRANSACTIONS} so that
   * the transactions table has only transactions involving accounts that transact
   * "frequently enough." Also creates a table containing all users and their total
   * number of transactions, "USERS"
   * <p>
   * NOTES:
   * --Filter out self-to-self transactions
   * --Throw out "supernode" #25
   *
   * @param connection
   * @return
   * @throws Exception
   */
  protected static void filterForActivity(String dbType, String h2DatabaseName) throws Exception {
    DBConnection connection = new IngestSql().getDbConnection(dbType, h2DatabaseName);

    long then = System.currentTimeMillis();
    logger.info("filterForActivity start");
    filterForActivity(connection);
    long now = System.currentTimeMillis();

    logger.info("filterForActivity end took " + (now - then) + " millis");

    connection.closeConnection();
  }

  /**
   * Overloaded {@link #filterForActivity(String, String)}
   * <p>
   * Filter out accounts involved in fewer than {@link #MIN_TRANSACTIONS} so that
   * the transactions table has only transactions involving accounts that transact
   * "frequently enough." Also creates a table containing all users and their total
   * number of transactions, "USERS"
   * <p>
   * NOTES:
   * --Filter out self-to-self transactions
   * --Throw out "supernode" #25
   *
   * @param connection
   * @throws Exception
   */
  private static void filterForActivity(DBConnection connection) throws Exception {
		
		/*
		 * Setup SQL queries
		 */
//		String sqlRemoveAccountOld = "delete from "+TABLE_NAME+" where"+
//				" (source = "+BITCOIN_OUTLIER+") or"+ 
//				" (target  = "+BITCOIN_OUTLIER+");";

    String sqlRemoveAccount = "delete from " + TABLE_NAME + " where";
    // loop-through all outliers we want to remove...
    for (int i = 0; i < BITCOIN_OUTLIERS.size(); i++) {
      sqlRemoveAccount += " (source = " + BITCOIN_OUTLIERS.get(i) + ") or" +
          " (target  = " + BITCOIN_OUTLIERS.get(i) + ")";

      if (i != BITCOIN_OUTLIERS.size() - 1)
        sqlRemoveAccount += " or";
    }
    //sqlRemoveAccount += ";";

    //  String sqlRemoveSelfTrans = "delete from " + TABLE_NAME + " where (source = target);";


		/*
		 * Do some initial clean-up
		 */
    logger.info("do remove account start from " + TABLE_NAME);
    doSQLUpdate(connection, sqlRemoveAccount);
    logger.info("did remove account start from " + TABLE_NAME);

/*
    logger.info("do remove self transactions " + sqlRemoveSelfTrans);

    doSQLUpdate(connection, sqlRemoveSelfTrans);*/

//    removeAccountsBelowThreshold(connection);

    //update user feature table to reflect filtering...
    //deleteInactive(connection);

    //cleanup
    // doSQLUpdate(connection, "drop table inactive");
  }

  private static void deleteInactive(DBConnection connection) throws SQLException {
    if (existsTable(connection, BitcoinBinding.USER_FEATURES_TABLE)) {
      String sqlDeleteInactiveUsers = "delete from " + BitcoinBinding.USER_FEATURES_TABLE +
          " where user not in" +
          " (select source from " + TABLE_NAME + " union select target from " + TABLE_NAME + ");";
      doSQLUpdate(connection, sqlDeleteInactiveUsers);
    } else {
      logger.info("table " + BitcoinBinding.USER_FEATURES_TABLE + " does not yet exist.");
    }
  }

  private static void removeAccountsBelowThreshold(DBConnection connection) throws SQLException {

    String sqlFindInactive =
        "drop table if exists inactive;" +
            " create table inactive as" +
            " select uid, sum(cnt) as tot_count from" +
            " (select source as uid, count(*) as cnt from " + TABLE_NAME +
            " group by source" +
            " union" +
            " select target as uid, count(*) as cnt from " + TABLE_NAME +
            " group by target order by uid) as tbl" +
            " group by uid having tot_count < " + MIN_TRANSACTIONS + ";";

    String sqlCountInactive = "select count(*) as cnt from inactive;";

    String sqlRemoveInactiveAccounts = "delete from " + TABLE_NAME + " where" +
        " (source in (select distinct uid from inactive)) or" +
        " (target in (select distinct uid from inactive));";

    String sqlDeleteInactiveUsers = "delete from " + BitcoinBinding.USER_FEATURES_TABLE +
        " where user not in" +
        " (select source from " + TABLE_NAME + " union select target from " + TABLE_NAME + ");";
  /*
   * Find and remove non-active accounts until only active accounts remain in transactions table
   */
    int numInactive = 1;
    while (numInactive != 0) {
      //find inactive

      logger.info("do find inactive");

      doSQLUpdate(connection, sqlFindInactive);
      logger.info("do count inactive");

      //how many inactive accounts
      ResultSet rs = doSQLQuery(connection, sqlCountInactive);
      rs.next();
      numInactive = rs.getInt("CNT");
      logger.info("number of inactive acounts in " + TABLE_NAME + ": " + numInactive);
      queryStatement.close();

      //remove all inactive accounts

      logger.info("do remove inactive");

      doSQLUpdate(connection, sqlRemoveInactiveAccounts);
      logger.info("do removed inactive");
    }
  }


  /**
   * Do SQL, update something, return nothing
   *
   * @param connection
   * @param createSQL
   * @throws SQLException
   * @see #filterForActivity(DBConnection)
   */
  private static int doSQLUpdate(DBConnection connection, String createSQL) throws SQLException {
    Connection connection1 = connection.getConnection();
    PreparedStatement statement = connection1.prepareStatement(createSQL);
    int i = statement.executeUpdate();
    statement.close();
    return i;
  }

  /**
   * Do SQL, query something, return that something
   *
   * @param connection
   * @param createSQL
   * @return
   * @throws SQLException
   * @see #existsTable(DBConnection, String)
   * @see #removeAccountsBelowThreshold(DBConnection)
   */
  private static ResultSet doSQLQuery(DBConnection connection, String createSQL) throws SQLException {
    Connection connection1 = connection.getConnection();
    //PreparedStatement statement = connection1.prepareStatement(createSQL);
    queryStatement = connection1.prepareStatement(createSQL);
    ResultSet rs = queryStatement.executeQuery();
    //statement.close();
    return rs;
  }

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


  /**
   * @param args
   */
  public static void main(String[] args) throws Throwable {
		
		/*
		 * Pre-processing the graph to prepare it for topk-subgraph ingest/indexing 
		 */
    String bitcoinDirectory = "src/main/resources" + BitcoinBinding.BITCOIN_FEATS_TSV;

    DBConnection dbConnection = new H2Connection(bitcoinDirectory, "bitcoin");

    // Filter-out non-active nodes, self-transitions, heavy-hitters
    filterForActivity(dbConnection);

    // Create marginalized graph data and various stats
    extractUndirectedGraph(dbConnection);

    //Do the indexing for the topk-subgraph algorithm
    computeIndices(bitcoinDirectory, dbConnection);

    // Do some querying (from example query files) based on some input graph
    //executeQuery(bitcoinDirectory,dbConnection);
  }

}
