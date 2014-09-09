package mitll.xdata.binding;

import influent.idl.*;
import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;

import org.apache.log4j.Logger;

import uiuc.topksubgraph.Graph;
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

	private int K = 500;
	private int D = 2;
	
	private String usersTable = "users";
	private String userIdColumn = "user";
	private String typeColumn = "type";
	
	private String graphTable = "MARGINAL_GRAPHH";
	private String pairIDColumn = "sorted_pairr";

	private QueryExecutor executor;

	//private static Connection connection;
	private static PreparedStatement queryStatement;




	public TopKSubgraphShortlist(Binding binding) {
		super(binding);

		//setup shortlist stuff here having to do with state...
		logger.info("setting up query executor... this should only happen once...");
		executor = new QueryExecutor();

		QueryExecutor.datasetId = binding.datasetId;
		QueryExecutor.baseDir="src/main/resources" + binding.datasetResourceDir; //THIS LINE SHOULD CHANGE FOR JAR-ed VERSION
		//QueryExecutor.k0 = D;
		//QueryExecutor.topK= K;

		//load graph (this may just need to be rolled up into BitcoinBinding)
		logger.info("Loading graph. This should only happen once...");
		Graph g = new Graph();
		try {
			g.loadGraph(binding.connection, "MARGINAL_GRAPH", "NUM_TRANS");
		} catch (Exception e) {
			logger.info("Got: "+e);
		}
		executor.g = g;

		QueryExecutor.spathFile=QueryExecutor.datasetId+"."+QueryExecutor.k0+".spath";
		QueryExecutor.topologyFile=QueryExecutor.datasetId+"."+QueryExecutor.k0+".topology";
		QueryExecutor.spdFile=QueryExecutor.datasetId+"."+QueryExecutor.k0+".spd";
		QueryExecutor.resultDir="results";

		windowsPathnamePortabilityCheck();
	}
	
	
	/**
	 * Method to set algorithm parameters once they've been set externally
	 */
	public void refreshQueryExecutorParameters() {
		QueryExecutor.k0 = D;
		QueryExecutor.topK= K;
	}
	
	/**
	 * Load a whole bunch of stuff from:
	 * 	db: node type data
	 * 	file: and all pre-computed indices
	 */
	public void loadTypesAndIndices() {
		/*
		 * Load-in types, and count how many there are
		 */
		try {
			logger.info("Loading in types...");
			executor.loadTypesFromDatabase(binding.connection,usersTable,userIdColumn,typeColumn);
			
			logger.info("Loading in indices...");
			executor.loadGraphNodesType();		//compute ordering
			executor.loadGraphSignatures();		//topology
			executor.loadEdgeLists();			//sorted edge lists
			executor.loadSPDIndex();			//spd index


			//Make resultDir if necessary
			File directory = new File(QueryExecutor.baseDir+QueryExecutor.resultDir);
			if (!directory.exists() && !directory.mkdirs()) 
				throw new IOException("Could not create directory: " + QueryExecutor.baseDir+QueryExecutor.resultDir);

		} 
		catch (IOException e) {logger.info("Got IOException: "+e);} 
		catch (Exception e) {logger.info("Got Exception: "+e);} 
		catch (Throwable e) {logger.info("Got Throwable: "+e);}
	}
	

	@Override
	public List<FL_PatternSearchResult> getShortlist(List<FL_EntityMatchDescriptor> entities1, 
			List<String> exemplarIDs, long max) {

		// which binding are we bound to?
		logger.info(this.binding.toString());
		logger.info(this.binding.connection);


		/*
		 * try out shortlist from query file... should work most likely...
		 */
		String[] queries = new String[]{"FanOut" , "FOFI"};
		for (int i=0; i<queries.length; i++) {
			
			/*
			 * Setup and read-in query
			 */
			QueryExecutor.queryFile="queries/queryGraph."+queries[i]+".txt";
			QueryExecutor.queryTypesFile="queries/queryTypes."+queries[i]+".txt";
			
			//set system out to out-file...
			try {
				System.setOut(new PrintStream(new File(QueryExecutor.baseDir+QueryExecutor.resultDir+
						"/QueryExecutor.topK="+QueryExecutor.topK+
						"_K0="+QueryExecutor.k0+
						"_"+QueryExecutor.datasetId+
						"_"+QueryExecutor.queryFile.split("/")[1])));
			} catch (FileNotFoundException e) {
				logger.info("got: "+e);
			}
			
			
			int isClique=0;
			try {
				isClique = executor.loadQuery();
			} 
			catch (NumberFormatException e) {logger.info("got NumberFormatException: "+e);} 
			catch (FileNotFoundException e) {logger.info("got FileNotFoundException: "+e);}
			catch (IOException e) {logger.info("got IOException: "+e);}
			catch (Throwable e) {logger.info("got Throwable: "+e);}

			/**
			 * Get query signatures
			 */
			executor.getQuerySignatures(); //fills in querySign

			/**
			 * NS Containment Check and Candidate Generation
			 */
			long time1=new Date().getTime();

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

			long time2=new Date().getTime();
			System.out.println("Overall Time: "+(time2-time1));


			//FibonacciHeap<ArrayList<String>> queryResults = executor.getHeap();
			executor.printHeap();
		}
		

		//check to see if we can connect to anything....
		if (existsTable(graphTable)) {
			logger.info("we can connect to the right database...");
		} else {
			logger.info("table "+graphTable+" does not yet exist.");
		}

		/*
		 * Get all pairs of query nodes...
		 * (this is assuming ids are sortable by integer comparison, like in bitcoin)
		 */
		int e1, e2;
		String pair;
		if (exemplarIDs.size() > 1) {
			for (int i=0;i<exemplarIDs.size();i++){
				for (int j=i+1;j<exemplarIDs.size();j++) {
					e1 = Integer.parseInt(exemplarIDs.get(i));
					e2 = Integer.parseInt(exemplarIDs.get(j));
					if (Integer.parseInt(exemplarIDs.get(i)) <= Integer.parseInt(exemplarIDs.get(j))) {
						pair = "("+e1+","+e2+")";
					} else {
						pair = "("+e2+","+e1+")";
					}

					if (existsPair(graphTable,pairIDColumn,pair)) {
						logger.info("pair: "+pair);
						// do some loading of the query...
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
	private boolean existsTable(String tableName) {
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
	 * @param tableName
	 * @param pairID
	 * @return
	 */
	private boolean existsPair(String tableName, String pairIdCol, String pair) {
		try {
			String sql = "select count(*) as CNT from "+tableName+" where "+pairIdCol+" = "+pair+" limit 1;";
			ResultSet rs = doSQLQuery(sql); rs.next();
			int cnt = rs.getInt("CNT");
			rs.close();
			queryStatement.close();

			if (cnt > 0) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			logger.info("got e: "+e);
			return false;
		}
	}
	
	/**
	 * Do SQL, update something, return nothing
	 * 
	 * @param createSQL
	 * @throws SQLException
	 */
	private void doSQLUpdate(String sql) throws SQLException {
		//PreparedStatement statement = connection.prepareStatement(sql);
		PreparedStatement statement = this.binding.connection.prepareStatement(sql);
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
		userIdColumn = userIdColumn;
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


	/**
	 * If we're on Windows, convert file-separators to "/"
	 * (this should probably be in an utilities package at some point)
	 */
	private static void windowsPathnamePortabilityCheck() {
		String osName = System.getProperty("os.name");

		if (osName.startsWith("Windows")) {
			String separator = Pattern.quote(System.getProperty("file.separator"));

			QueryExecutor.baseDir=QueryExecutor.baseDir.replace(separator, "/");
			QueryExecutor.graphFile=QueryExecutor.graphFile.replace(separator, "/");
			QueryExecutor.typesFile=QueryExecutor.typesFile.replace(separator, "/");
			QueryExecutor.queryFile=QueryExecutor.queryFile.replace(separator, "/");
			QueryExecutor.queryTypesFile=QueryExecutor.queryTypesFile.replace(separator, "/");
			QueryExecutor.spathFile=QueryExecutor.spathFile.replace(separator, "/");
			QueryExecutor.topologyFile=QueryExecutor.topologyFile.replace(separator, "/");
			QueryExecutor.spdFile=QueryExecutor.spdFile.replace(separator, "/");
			QueryExecutor.resultDir=QueryExecutor.resultDir.replace(separator, "/");
		}
	}

}
