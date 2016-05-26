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

package mitll.xdata.dataset.bitcoin.binding;

import influent.idl.*;
import mitll.xdata.AvroUtils;
import mitll.xdata.NodeSimilaritySearch;
import mitll.xdata.ServerProperties;
import mitll.xdata.binding.Binding;
import mitll.xdata.binding.TopKSubgraphShortlist;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.hmm.VectorObservation;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * Created with IntelliJ IDEA. User: go22670 Date: 7/10/13 Time: 4:08 PM To change this template use File | Settings |
 * File Templates.
 */
public class BitcoinBinding extends Binding {
  private static final Logger logger = Logger.getLogger(BitcoinBinding.class);

//  public static final String DATASET_ID = "bitcoin_small";
//  public static final String BITCOIN_FEATS_TSV = "/bitcoin_small_feats_tsv/"; // TODO : make sure ingest writes to this directory.

  //private static final String BITCOIN_FEATURES = BitcoinFeatures.BITCOIN_FEATURES_STANDARDIZED_TSV;
  public static final String TRANSACTIONS = "transactions";


  private static final int BUCKET_SIZE = 2;
  // private static final String BITCOIN_IDS = BITCOIN_FEATS_TSV + BitcoinFeatures.BITCOIN_IDS_TSV;
  private static final String TRANSID = "transid";


  private NodeSimilaritySearch userIndex;

  private PreparedStatement pairConnectedStatement;
  private PreparedStatement edgeMetadataKeyStatement;
  private boolean useFastBitcoinConnectedTest = true;

  // TopKSubgraphShortlist Parameters
  private static final int DEFAULT_SHORTLIST_SIZE = 1000;
  public static final int SHORTLISTING_D = 2;

  public static final String USER_FEATURES_TABLE = "users";
  private static final String USERID_COLUMN = "user";
  private static final String TYPE_COLUMN = "type";

  private static final String GRAPH_TABLE = "MARGINAL_GRAPH";
  private static final String PAIRID_COLUMN = "SORTED_PAIR";

  /**
   * @param connection
   * @param props
   * @see mitll.xdata.GraphQuBEServer#main(String[])
   */
  public BitcoinBinding(DBConnection connection, ServerProperties props) {
    this(connection, true, props);
  }

  /**
   * @param connection
   * @param useFastBitcoinConnectedTest
   * @param props
   * @throws Exception
   * @see #BitcoinBinding(DBConnection, boolean, ServerProperties)
   */
  private BitcoinBinding(DBConnection connection, boolean useFastBitcoinConnectedTest, ServerProperties props) {
    super(connection);

    datasetId = props.getDatasetID();
    datasetResourceDir = props.getDatasetResourceDir();

		/* 
     * Setup Shortlist-er
		 */
    shortlist = new TopKSubgraphShortlist(this);

    // Set and update all TopKSubgraphShortlist specific parameters
    if (getShortlist() instanceof TopKSubgraphShortlist) {
      TopKSubgraphShortlist shortlist = (TopKSubgraphShortlist) getShortlist();

      shortlist.setK(DEFAULT_SHORTLIST_SIZE);
      shortlist.setD(SHORTLISTING_D);
      shortlist.refreshQueryExecutorParameters();
      shortlist.setUsersTable(USER_FEATURES_TABLE);
      shortlist.setUserIdColumn(USERID_COLUMN);
      shortlist.setTypeColumn(TYPE_COLUMN);
      shortlist.setGraphTable(GRAPH_TABLE);
      shortlist.setPairIDColumn(PAIRID_COLUMN);

      //load types and indices
      shortlist.loadTypesAndIndices();
    }

    this.useFastBitcoinConnectedTest = useFastBitcoinConnectedTest;

    prefixToTable.put("t", TRANSACTIONS);
    tableToPrimaryKey.put(TRANSACTIONS, "SOURCE");

    for (String col : tableToPrimaryKey.values())
      addTagToColumn(FL_PropertyTag.ID, col);
    // addTagToColumn(FL_PropertyTag.ID, "TARGET");
    addTagToColumn(FL_PropertyTag.DATE, "TIME");
    addTagToColumn(FL_PropertyTag.AMOUNT, "AMOUNT");
    tableToDisplay.put(TRANSACTIONS, TRANSACTIONS);
    Collection<String> tablesToQuery = prefixToTable.values();
    tablesToQuery = new ArrayList<String>(tablesToQuery);

    populateTableToColumns(connection, tablesToQuery, connection.getType());
    populateColumnToTables();

    // logger.debug("cols " + tableToColumns);

    //setupForOldSubgraphSearch(connection, useFastBitcoinConnectedTest, resourceDir);
  }
/*
  private void setupForOldSubgraphSearch(DBConnection connection, boolean useFastBitcoinConnectedTest, String resourceDir) {
    try {
      //  InputStream userFeatures = this.getClass().getResourceAsStream(File.separator +resourceDir + File.separator +BITCOIN_FEATURES);
      logger.debug("indexing node features");
      long then = System.currentTimeMillis();
      logger.debug(File.separator + resourceDir + File.separator + BITCOIN_FEATURES);
      //userIndex = new NodeSimilaritySearch("/" +resourceDir + "/" +BITCOIN_FEATURES);
      userIndex = new NodeSimilaritySearch(resourceDir + "/" + BITCOIN_FEATURES);
      long now = System.currentTimeMillis();

      logger.debug("done indexing node features in " + (now - then) + " millis");

      if (useFastBitcoinConnectedTest) {
        populateInMemoryAdjacency();
      } else {
        // create prepared statement for determining if two nodes connected
        makePairConnectedStatement(connection);
      }
      makeEdgeMetadataStatement(connection);
    } catch (Exception e) {
      logger.error("got " + e, e);
    }
  }*/

  @Override
  public int compareEntities(String e1, String e2) {
    if (Integer.parseInt(e1) < Integer.parseInt(e2)) {
      return -1;
    } else if (Integer.parseInt(e1) > Integer.parseInt(e2)) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public Map<String, String> getEdgeAttributes(String src, String dest, Set<String> columnNames) {
    Map<String, String> attributes = new HashMap<String, String>();

    String sql = "select ";
    int count = 0;
    for (String columnName : columnNames) {
      if (count == 0) {
        sql += columnName;
      } else {
        sql += ", " + columnName;
      }
      count += 1;
    }
//    sql += " from "+GRAPH_TABLE+" where "+PAIRID_COLUMN+" = ("+src+","+dest+");";
    sql += " from " + GRAPH_TABLE + " where " + "SOURCE=" + src + " AND TARGET=" + dest;

    PreparedStatement queryStatement;

    try {
      queryStatement = connection.prepareStatement(sql);
      ResultSet rs = queryStatement.executeQuery();
      rs.next();

      for (String columnName : columnNames) {
        // retrieve from rs as appropriate type
        //String attributeType = edgeAttributeName2Type.get(columnName);

        //			  if (attributeType == "INTEGER") {
        //				  attributes.put(columnName, rs.getInt(columnName));
        //			  } else if (attributeType == "ID_COL_TYPE") {
        //				  attributes.put(columnName, rs.getLong(columnName));
        //			  }


        //					h2Type2InfluentType.put("INTEGER", FL_PropertyType.LONG);
        //				h2Type2InfluentType.put("ID_COL_TYPE", FL_PropertyType.LONG);
        //				h2Type2InfluentType.put("DOUBLE", FL_PropertyType.DOUBLE);
        //				h2Type2InfluentType.put("DECIMAL", FL_PropertyType.DOUBLE);
        //				h2Type2InfluentType.put("VARCHAR", FL_PropertyType.STRING);
        //				h2Type2InfluentType.put("ARRAY", FL_PropertyType.OTHER);


        attributes.put(columnName, rs.getString(columnName));
      }

      rs.close();
      queryStatement.close();

    } catch (SQLException e) {
      logger.info("Got e: " + e);
    }

    return attributes;
  }


  private void makeEdgeMetadataStatement(DBConnection connection) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("select * from (");
    sql.append(" select transid");
    sql.append(" from transactions");
    sql.append(" where (source = ? and target = ?)");
    sql.append(" limit 1");
    sql.append(" ) as temp");
    edgeMetadataKeyStatement = connection.getConnection().prepareStatement(sql.toString());
  }

  private void makePairConnectedStatement(DBConnection connection) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("select count(1) from (");
    sql.append(" select 1");
    sql.append(" from transactions");
    sql.append(" where (source= ? and target = ?)");
    sql.append(" limit 1");
    sql.append(" ) as temp");
    pairConnectedStatement = connection.getConnection().prepareStatement(sql.toString());
  }

  /**
   * Read from the connected pairs file to create an adjacency matrix.
   *
   * @throws Exception
   */
  private void populateInMemoryAdjacency() throws Exception {
    InputStream pairs = this.getClass().getResourceAsStream("/bitcoin_feats_tsv/pairs.txt");
    long then = System.currentTimeMillis();

    BufferedReader br = new BufferedReader(new InputStreamReader(pairs));
    String line;
    int count = 0;
    long t0 = System.currentTimeMillis();
    int max = Integer.MAX_VALUE;
    int bad = 0;

    while ((line = br.readLine()) != null) {
      count++;
      if (count > max)
        break;

      long e = Long.parseLong(line);
      long[] recover = recover(e);

      int source = (int) recover[0];
      String sSource = "" + source;
      int target = (int) recover[1];

      String sTarget = "" + target;

      Set<String> targetsForSource = stot.get(sSource);
      if (targetsForSource == null)
        stot.put(sSource, targetsForSource = new HashSet<String>());
      if (!targetsForSource.contains(sTarget)) {
        targetsForSource.add(sTarget);
        validTargets.add(sTarget);
      }

      if (count % 1000000 == 0) {
        logger.debug("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count + " ms/read");
      }
    }
    br.close();
    logger.debug("populateInMemoryAdjacency : took  " + (System.currentTimeMillis() - then) + " millis to read "
        + count + " entries.");

  }

  /**
   * @param example
   * @return
   * @see mitll.xdata.GraphQuBEServer#getRoute(mitll.xdata.SimplePatternSearch)
   */
  @Override
  public List<Binding.ResultInfo> getEntities(FL_PatternDescriptor example) {
    List<String> exemplarIDs = Binding.getExemplarIDs(example);
    List<Binding.ResultInfo> entities = new ArrayList<Binding.ResultInfo>();
    for (String id : exemplarIDs) {
      char c = id.charAt(0);
      if (c != 't') {
        id = "t" + id;
      }
      Binding.ResultInfo entity = getEntityResult(id);
      entities.add(entity);
    }
    return entities;
  }

  /**
   * For some reason making a hash set of longs is really slow.
   *
   * @throws SQLException
   * @paramx connection
   * @deprecated
   */
  /*    private void getPairs(DBConnection connection) throws SQLException {
        PreparedStatement pairConnectedStatement2 = connection.getConnection().prepareStatement(
                "select source, target from transactions");

        long then = System.currentTimeMillis();
        ResultSet rs;
        rs = pairConnectedStatement2.executeQuery();
        int c = 0;
        while (rs.next()) {
            long source = rs.getLong(1);
            long target = rs.getLong(2);

            // connectedPairs.add(storeTwo(source, target));
            if (c++ % 1000000 == 0)
                logger.debug("read " + c);
        }
        rs.close();
        pairConnectedStatement2.close();
        logger.debug("took  " + (System.currentTimeMillis() - then) + " millis");
    }*/
	/*  private long storeTwo(long low, long high) {
    long combined = low;
    combined += high << 32;
    return combined;
  }*/

  /**
   * split a long into two integers - high and low
   *
   * @param combined
   * @return
   * @see
   */
  private long[] recover(long combined) {
    long low = combined & Integer.MAX_VALUE;
    long high = (combined >> 32) & Integer.MAX_VALUE;
    long[] both = new long[2];

    both[0] = low;
    both[1] = high;
    return both;
  }

  /**
   * @param id
   * @param k
   * @param skipSelf
   * @return
   * @see Binding#searchByExample(FL_PatternDescriptor, String, long, long, boolean, List)
   */
  @Override
  protected List<String> getNearestNeighbors(String id, int k, boolean skipSelf) {
    List<String> neighbors = userIndex.neighbors(id, k);
    if (skipSelf && neighbors.size() >= 1 && neighbors.get(0).equals(id)) {
      neighbors.remove(0);
    }
    return neighbors;
  }

  /**
   * @param id1
   * @param id2
   * @return
   * @see Binding#searchByExample(FL_PatternDescriptor, String, long, long, boolean, List)
   */
  @Override
  protected double getSimilarity(String id1, String id2) {
    return userIndex.similarity(id1, id2);
  }

  /**
   * @param i
   * @param j
   * @return
   * @throws Exception
   * @see mitll.xdata.binding.CartesianShortlist#connectedGroup
   */
  @Override
  protected boolean isPairConnected(String i, String j) throws Exception {
    if (useFastBitcoinConnectedTest) {
      return inMemoryIsPairConnected(i, j);
    } else {
      // logger.debug("isPairConnected(" + i + ", " + j + ")");
      int parameterIndex;
      ResultSet rs;
      // i is source
      parameterIndex = 1;
      pairConnectedStatement.setInt(parameterIndex++, Integer.parseInt(i, 10));
      pairConnectedStatement.setInt(parameterIndex++, Integer.parseInt(j, 10));
      rs = pairConnectedStatement.executeQuery();
      if (rs.next()) {
        if (rs.getInt(1) > 0) {
          return true;
        }
      }
      // i is target
      parameterIndex = 1;
      pairConnectedStatement.setInt(parameterIndex++, Integer.parseInt(j, 10));
      pairConnectedStatement.setInt(parameterIndex++, Integer.parseInt(i, 10));
      rs = pairConnectedStatement.executeQuery();
      if (rs.next()) {
        if (rs.getInt(1) > 0) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Check either order -- first node as source or second
   *
   * @param f
   * @param s
   * @return
   * @see mitll.xdata.dataset.bitcoin.binding.BitcoinBinding#isPairConnected(String, String)
   */
  private boolean inMemoryIsPairConnected(String f, String s) {
    Set<String> integers = stot.get(f);
    if (integers != null && integers.contains(s)) {
      return true;
    }

    Set<String> integers2 = stot.get(s);
    return integers2 != null && integers2.contains(f);
  }

  /**
   * @param id1
   * @param id2
   * @return
   * @throws Exception
   * @see mitll.xdata.binding.Shortlist#getLinks
   */
  @Override
  protected String getEdgeMetadataKey(String id1, String id2) throws Exception {
    int parameterIndex;
    ResultSet rs;
    // i as source
    parameterIndex = 1;
    edgeMetadataKeyStatement.setInt(parameterIndex++, Integer.parseInt(id1, 10));
    edgeMetadataKeyStatement.setInt(parameterIndex++, Integer.parseInt(id2, 10));
    rs = edgeMetadataKeyStatement.executeQuery();
    if (rs.next()) {
      return rs.getString(1);
    }
    // i as target
    parameterIndex = 1;
    edgeMetadataKeyStatement.setInt(parameterIndex++, Integer.parseInt(id2, 10));
    edgeMetadataKeyStatement.setInt(parameterIndex++, Integer.parseInt(id1, 10));
    rs = edgeMetadataKeyStatement.executeQuery();
    if (rs.next()) {
      return rs.getString(1);
    }
    return null;
  }

  @Override
  protected FL_Property createEdgeMetadataKeyProperty(String id) {
    return createProperty(TRANSID, Long.parseLong(id), FL_PropertyType.LONG);
  }

  @Override
  public List<Edge> getAllLinks(List<String> ids) {
    long t0 = System.currentTimeMillis();

    List<Edge> edges = new ArrayList<Edge>();

    if (ids.size() == 1) {
      return edges;
    }

    // TODO : make limit configurable?

    String sql = "";
    sql += "select transid, source, target, time, amount, usd, devpop, creditdev, debitdev";
    sql += " from transactions";
    sql += " where source = ? and target = ?";
    sql += " limit 250";

    try {
      PreparedStatement statement = connection.prepareStatement(sql);

      for (int i = 0; i < ids.size(); i++) {
        for (int j = 0; j < ids.size(); j++) {
          if (i == j) {
            continue;
          }
          // long start = System.currentTimeMillis();
          int parameterIndex = 1;
          statement.setInt(parameterIndex++, Integer.parseInt(ids.get(i), 10));
          statement.setInt(parameterIndex++, Integer.parseInt(ids.get(j), 10));
          ResultSet rs = statement.executeQuery();
          // int ijEdges = 0;
          while (rs.next()) {
            // ijEdges++;
            int source = rs.getInt(2);
            int target = rs.getInt(3);
            long time = rs.getLong(4);
            double amount = rs.getDouble(5);
            double usd = rs.getDouble(6);
            double devpop = rs.getDouble(7);
            double creditdev = rs.getDouble(8);
            double debitdev = rs.getDouble(9);
            BitcoinEdge edge = new BitcoinEdge(source, target, time, amount, usd, devpop, creditdev,
                debitdev);
            edges.add(edge);
          }
          // long stop = System.currentTimeMillis();
          // logger.debug("(" + i + ", " + j + ") took " + (start - stop) + " ms for " + ijEdges + " edges");
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return new ArrayList<Edge>();
    }

    long t1 = System.currentTimeMillis();
    if (t1 - t0 > 100)
      logger.debug("retrieving " + edges.size() + " edge(s) took " + (t1 - t0) + " ms");

    return edges;
  }

  @Override
  protected List<Edge> getAllLinks(List<String> ids, long startTime, long endTime) {
    // TODO: make this method obey startTime and endTime
    long t0 = System.currentTimeMillis();

    List<Edge> edges = new ArrayList<Edge>();

    if (ids.size() == 1) {
      return edges;
    }

    // TODO : make limit configurable?

    String sql = "";
    sql += "select transid, source, target, time, amount, usd, devpop, creditdev, debitdev";
    sql += " from transactions";
    sql += " where source = ? and target = ? and time >= ? and time <= ?";
    sql += " limit 250";

    try {
      PreparedStatement statement = connection.prepareStatement(sql);

      for (int i = 0; i < ids.size(); i++) {
        for (int j = 0; j < ids.size(); j++) {
          if (i == j) {
            continue;
          }
          // long start = System.currentTimeMillis();
          int parameterIndex = 1;
          statement.setInt(parameterIndex++, Integer.parseInt(ids.get(i), 10));
          statement.setInt(parameterIndex++, Integer.parseInt(ids.get(j), 10));
          statement.setLong(parameterIndex++, startTime);
          statement.setLong(parameterIndex++, endTime);
          ResultSet rs = statement.executeQuery();
          // int ijEdges = 0;
          while (rs.next()) {
            // ijEdges++;
            int source = rs.getInt(2);
            int target = rs.getInt(3);
            long time = rs.getLong(4);
            double amount = rs.getDouble(5);
            double usd = rs.getDouble(6);
            double devpop = rs.getDouble(7);
            double creditdev = rs.getDouble(8);
            double debitdev = rs.getDouble(9);
            BitcoinEdge edge = new BitcoinEdge(source, target, time, amount, usd, devpop, creditdev,
                debitdev);
            edges.add(edge);
          }
          // long stop = System.currentTimeMillis();
          // logger.debug("(" + i + ", " + j + ") took " + (start - stop) + " ms for " + ijEdges + " edges");
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return new ArrayList<Edge>();
    }

    long t1 = System.currentTimeMillis();
    if (t1 - t0 > 100)
      logger.debug("retrieving " + edges.size() + " edge(s) took " + (t1 - t0) + " ms");

    return edges;
  }

  /**
   * @param edges
   * @return
   * @see #rescoreWithHMM
   * @deprecated not used currently
   */
/*	private List<Transaction> createFeatureVectors(List<Edge> edges, List<String> exemplarIDs) {
		// sort edges
		Collections.sort(edges, new Comparator<Edge>() {
			@Override
			public int compare(Edge e1, Edge e2) {
				long t1 = ((BitcoinEdge) e1).time;
				long t2 = ((BitcoinEdge) e2).time;
				if (t1 < t2) {
					return -1;
				} else if (t1 > t2) {
					return 1;
				} else {
					return 0;
				}
			}
		});

		List<Transaction> transactions = new ArrayList<Transaction>();

		BitcoinEdge firstEdge = (BitcoinEdge) edges.get(0);
		BitcoinEdge lastEdge = (BitcoinEdge) edges.get(edges.size() - 1);
		long eventLength = lastEdge.getTime() - firstEdge.getTime();
		// initialize previous time and amount to first edge's values
		long prevTime = firstEdge.getTime();
		double prevAmount = firstEdge.getAmount();

		for (Edge temp : edges) {
			BitcoinEdge edge = (BitcoinEdge) temp;
			double[] features = new double[5];

			// direct from database
			features[0] = edge.getDeviationFromPopulation();
			features[1] = edge.getDeviationFromOwnCredits();
			features[2] = edge.getDeviationFromOwnDebits();

			// time since previous transaction (in event), normalized by length of event
			long currentTime = edge.getTime();
			features[3] = (currentTime - prevTime) / (1.0 * eventLength);
			prevTime = currentTime;

			// relative amount vs previous
			double currentAmount = edge.getAmount();
			features[4] = currentAmount / prevAmount;
			prevAmount = currentAmount;

			transactions.add(new Transaction("" + edge.getSource(), "" + edge.getTarget(), edge.getTime(), features));
		}

		return transactions;
	}*/

  /**
   * @param edges
   * @param ids
   * @return
   * @see #getResultObservations(java.util.List, java.util.List, java.util.List)
   * @see #rescoreWithHMM(java.util.List, java.util.List, java.util.List, java.util.List, java.util.List)
   */
  @Override
  protected List<VectorObservation> createObservationVectors(List<Edge> edges, List<String> ids) {

//		logger.info("=========debugging createObservationVectors()========================="); 

    //
    // sort edges
    //
    Collections.sort(edges, new Comparator<Edge>() {
      @Override
      public int compare(Edge e1, Edge e2) {
        long t1 = e1.getTime();
        long t2 = e2.getTime();
        if (t1 < t2) {
          return -1;
        } else if (t1 > t2) {
          return 1;
        } else {
          return 0;
        }
      }
    });
//		logger.info("sorted edges ("+edges.size()+")...");
//		logger.info(edges);


    //
    // make one observation per temporal bucket
    //
    List<VectorObservation> observations = new ArrayList<VectorObservation>();

		/*
		* map all pairs of query nodes to indices (that occur after initial fixed part of feature vector)
		*/
    Map<String, Integer> indexMap = new HashMap<String, Integer>();
    int numNodes = ids.size();
    int numFixedFeatures = 2; // i.e., that don't depend on number of nodes in query
    int index = numFixedFeatures;
    for (int i = 0; i < numNodes; i++) {
      for (int j = i + 1; j < numNodes; j++) {
        String key = ids.get(i) + ":" + ids.get(j);
        indexMap.put(key, index);
        index++;
      }
    }
//		logger.info("here comes indexMap:");
//		logger.debug("indexMap = " + indexMap);
//		logger.info("num of nodes: "+numNodes);


    BitcoinEdge firstEdge = (BitcoinEdge) edges.get(0);
    int edgeCount = 0;
    long lastTime = firstEdge.getTime();
    long lastBucketFirstTime = firstEdge.getTime();
    boolean newBucket = true;
    long thisBucketFirstTime = -1;

    double[] features = new double[2 + (numNodes * (numNodes - 1)) / 2];
//		logger.info("length of feature vector:"+features.length);
		
		/*
		 * fill in features for temporal "bucket" (what defines bucket though?)
		 */
    List<Edge> bucketEdges = new ArrayList<Edge>();
    for (Edge edge1 : edges) {
      edgeCount++;
      BitcoinEdge edge = (BitcoinEdge) edge1;
      bucketEdges.add(edge);

			/*
			 * EDGE-INDEPENDENT FEATURE ACCUMULATION
			 */

      //Accumulate mean time between transactions
      long thisTime = edge.getTime();

      if (!newBucket) {
        features[0] += (thisTime - lastTime);
//				logger.info("here's the first feature: "+features[0]);
      }

      // Time since last bucket (will be zero for very first bucket)
      if (newBucket) {
        features[1] = (thisTime - lastBucketFirstTime);
        thisBucketFirstTime = thisTime;
      }
      newBucket = false;
      lastTime = thisTime;

			
			/*
			 * EDGE-DEPENDENT FEATURES
			 */

      // Net Flow between Pairs of Nodes
      // TODO: normalize (later) by total flow within bucket???
      String key = edge.getSource() + ":" + edge.getTarget();
      if (indexMap.containsKey(key)) {
        // this index is for net flow from source to target (so add amount)
        features[indexMap.get(key)] += edge.getUsd();
      } else {
        // this index is for net flow from target to source (so subtract amount)
        key = edge.getTarget() + ":" + edge.getSource();
        features[indexMap.get(key)] -= edge.getUsd();
      }

			
			/*
			 * BUCKET_SIZE is the number of edges to accumulate before getting a feature vector out
			 * The minimum value this can take is 2....
			 */
      if (edgeCount == BUCKET_SIZE) {


        double denom = (lastTime - thisBucketFirstTime);
        if (denom > 0) {
          // normalize mean time between transactions by bucket length
          features[0] /= denom;
          features[1] /= denom;
        }
				
				/*
				// mean time between transactions in bucket...
				features[0] /= edgeCount-1;
		
				
				// normalize time since last bucket by total length spanned by both buckets
				// Note: features[1] is already zero for very first bucket, so this doesn't do anything
				features[1] /= (lastTime - lastBucketFirstTime);
				*/

        // add observation
        VectorObservation observation = new VectorObservation(features);
        observation.setEdges(bucketEdges);
        observations.add(observation);

        String featString = "";
        for (int i = 0; i < features.length; i++) {
          featString += "\t" + features[i];
        }
//				logger.info(featString);	

        // reset things
        newBucket = true;
        edgeCount = 0;
        bucketEdges = new ArrayList<Edge>();
        lastBucketFirstTime = thisBucketFirstTime;
        features = new double[2 + (numNodes * (numNodes - 1)) / 2];
      }
    }

    // finish last bucket if necessary
    if (edgeCount > 0) {
      // logger.debug("last bucket: edgeCount = " + edgeCount);
      // logger.debug("last bucket: lastTime = " + lastTime);
      // logger.debug("last bucket: thisBucketFirstTime = " + thisBucketFirstTime);

      // normalize mean time between transactions by bucket length
      double denom = (lastTime - thisBucketFirstTime);
      if (denom > 0) {
        features[0] /= (lastTime - thisBucketFirstTime);

        // normalize time since last bucket by total length spanned by both buckets
        // Note: features[1] is already zero for very first bucket, so this doesn't do anything
        features[1] /= (lastTime - lastBucketFirstTime);
      }


      // add observation
      VectorObservation observation = new VectorObservation(features);
      observation.setEdges(bucketEdges);
      observations.add(observation);

      String featString = "";
      for (int i = 0; i < features.length; i++) {
        featString += "\t" + features[i];
      }
//			logger.info(featString);	
    }


//		logger.info("=========debugging createObservationVectors()=========================");

    return observations;
  }

  /**
   * @param example
   * @param result
   * @param edges
   * @see #searchByExample
   */
  @Override
  protected List<FL_LinkMatchResult> createAggregateLinks(FL_PatternDescriptor example, FL_PatternSearchResult result, List<Edge> edges) {
    List<FL_LinkMatchResult> linkMatchResults = new ArrayList<FL_LinkMatchResult>();

    // map query uids to number associated with their place in query entity list
    // e.g., holds "QA --> 0", "QB --> 1"
    Map<String, Integer> queryIdToPosition = new HashMap<String, Integer>();
    for (int i = 0; i < example.getEntities().size(); i++) {
      FL_EntityMatchDescriptor entityMatchDescriptor = example.getEntities().get(i);
      queryIdToPosition.put(entityMatchDescriptor.getUid(), i);
      // logger.debug("exemplar uid = " + entityMatchDescriptor.getUid() + ", i = " + i);
    }

    // map result ids to the number associated with the query id
    // e.g., holds "RA' --> 0", "RB' --> 1"
    Map<String, Integer> resultIdToQueryIdPosition = new HashMap<String, Integer>();
    Map<Integer, String> positionToResultId = new HashMap<Integer, String>();
    for (FL_EntityMatchResult entityMatchResult : result.getEntities()) {
      String resultID = entityMatchResult.getEntity().getUid();
      String queryID = entityMatchResult.getUid();
      Integer queryPosition = queryIdToPosition.get(queryID);
      resultIdToQueryIdPosition.put(resultID, queryPosition);
      positionToResultId.put(queryPosition, resultID);
      // logger.debug("resultID = " + resultID + ", queryID = " + queryID + ", queryPosition = " + queryPosition);
    }

    // key is id1:id2 where id1 < id2 are query ID position
    // value is { #edges, out flow, in flow, net flow}
    Map<String, Object[]> pairStatistics = new HashMap<String, Object[]>();

    for (Edge temp : edges) {
      BitcoinEdge edge = (BitcoinEdge) temp;
      String source = "" + edge.getSource();
      String target = "" + edge.getTarget();
      double usd = edge.getUsd();
      int sourcePosition = resultIdToQueryIdPosition.get(source);
      int targetPosition = resultIdToQueryIdPosition.get(target);
      String key;
      double out;
      double in;
      if (sourcePosition < targetPosition) {
        // edge 2 --> 1, key 1:2, so flow is "out" (from source of key)
        key = sourcePosition + ":" + targetPosition;
        out = usd;
        in = 0.0;
      } else {
        // edge 2 --> 1, key 1:2, so flow is "in" (to source of key)
        key = targetPosition + ":" + sourcePosition;
        out = 0.0;
        in = usd;
      }
      Object[] object = pairStatistics.get(key);
      if (object == null) {
        object = new Object[4];
        object[0] = 0;
        object[1] = 0.0;
        object[2] = 0.0;
        object[3] = 0.0;
        pairStatistics.put(key, object);
      }
      object[0] = (Integer) object[0] + 1;
      object[1] = (Double) object[1] + out;
      object[2] = (Double) object[2] + in;
      object[3] = (Double) object[3] + (out - in);
    }

    for (Entry<String, Object[]> entry : pairStatistics.entrySet()) {
      String key = entry.getKey();
      List<String> fields = Binding.split(key, ":");
      String source = positionToResultId.get(Integer.parseInt(fields.get(0), 10));
      String target = positionToResultId.get(Integer.parseInt(fields.get(1), 10));

      Object[] value = entry.getValue();
      int numEdges = (Integer) value[0];
      double out = (Double) value[1];
      double in = (Double) value[2];
      double net = (Double) value[3];

      FL_LinkMatchResult linkMatchResult = new FL_LinkMatchResult();
      FL_Link link = new FL_Link();
      link.setSource(source);
      link.setTarget(target);
      //link.setTags(new ArrayList<FL_LinkTag>()); // Deprecated in Influent IDL 2.0
      List<FL_Property> properties = new ArrayList<FL_Property>();
      properties.add(createProperty("numEdges", numEdges, FL_PropertyType.LONG));
      properties.add(createProperty("outFlow", out, FL_PropertyType.DOUBLE));
      properties.add(createProperty("inFlow", in, FL_PropertyType.DOUBLE));
      properties.add(createProperty("netFlow", net, FL_PropertyType.DOUBLE));
      link.setProperties(properties);
      linkMatchResult.setLink(link);
      linkMatchResult.setScore(1.0);
      linkMatchResult.setUid("");
      linkMatchResults.add(linkMatchResult);
    }

    // result.setLinks(linkMatchResults);

    return linkMatchResults;
  }

  public static class BitcoinEdge implements Edge<Integer> {
    private final int source;
    private final int target;
    private final long time;
    private final double amount;
    private final double usd;

    /**
     * deviation from population mean usd
     */
    private final double deviationFromPopulation;

    /**
     * deviation from target's mean usd
     */
    private final double deviationFromOwnCredits;

    /**
     * deviation from source's mean usd
     */
    private final double deviationFromOwnDebits;

    public BitcoinEdge(int source, int target, long time, double amount, double usd,
                       double deviationFromPopulation, double deviationFromOwnCredits, double deviationFromOwnDebits) {
      this.source = source;
      this.target = target;
      this.time = time;
      this.amount = amount;
      this.usd = usd;
      this.deviationFromPopulation = deviationFromPopulation;
      this.deviationFromOwnCredits = deviationFromOwnCredits;
      this.deviationFromOwnDebits = deviationFromOwnDebits;
    }

    @Override
    public Integer getSource() {
      return source;
    }

    @Override
    public Integer getTarget() {
      return target;
    }

    @Override
    public long getTime() {
      return time;
    }

    public double getAmount() {
      return amount;
    }

    public double getUsd() {
      return usd;
    }

    public double getDeviationFromPopulation() {
      return deviationFromPopulation;
    }

    public double getDeviationFromOwnCredits() {
      return deviationFromOwnCredits;
    }

    public double getDeviationFromOwnDebits() {
      return deviationFromOwnDebits;
    }

    public String toString() {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
      String s = "";
      s += source;
      s += "\t" + target;
      s += "\t" + time;
      s += "\t" + sdf.format(new Date(time));
      s += "\t" + amount;
      s += "\t" + usd;
      s += "\t" + deviationFromPopulation;
      s += "\t" + deviationFromOwnCredits;
      s += "\t" + deviationFromOwnDebits;
      return s;
    }

    @Override
    public int compareTo(Edge o) {
      long t1 = this.time;
      long t2 = o.getTime();
      if (t1 < t2) {
        return -1;
      } else if (t1 > t2) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  private static String getPropsFile(String[] args) {
    String propsFile = null;
    for (String arg : args) {


      String prefix = "props=";
      logger.info("got " + arg);

      if (arg.startsWith(prefix)) {
        propsFile = getValue(arg, prefix);
      }
    }
    return propsFile;
  }


  private static String getValue(String arg, String prefix) {
    return arg.split(prefix)[1];
  }

  public static void main(String[] arg) throws Exception {
    System.getProperties().put("logging.properties", "log4j.properties");

    // testCombine();
    BitcoinBinding binding = null;
    try {
      binding = new BitcoinBinding(new H2Connection("bitcoin"), new ServerProperties(getPropsFile(arg)));
      //  binding = new BitcoinBinding(new H2Connection("c:/temp/bitcoin", "bitcoin"), true);
      // binding = new BitcoinBinding(new H2Connection( "bitcoin"), false);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    logger.debug("got " + binding);

    // List<String> ids = Arrays.asList(new String[] { "505134", "137750", "146073", "28946", "11" });
    // List<Edge> edges = binding.getAllLinks(ids);

    // List<FL_PatternSearchResult> results = binding.Aptima(0);
    // logger.debug("results.size() = " + results.size());
    // logger.debug(results.get(0));

    FL_PatternDescriptor descriptor = null;
    Object result;

    // use first aptima result as exemplar

    // query_0
    // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "505134", "137750", "146073",
    // "28946", "11" }));

    // query_1
    // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "97409", "11" }));

    // query_2
    descriptor = AvroUtils.createExemplarQuery(Arrays.asList("11", "1598539", "988143"));

    // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "505134", "137750", "146073",
    // "28946",
    // "11" }));

    boolean hmmScoring = true;
    // use aptima precomputed results
    logger.debug("descriptor = " + AvroUtils.encodeJSON(descriptor));
    result = binding.searchByExample(descriptor, 0, 100, hmmScoring, Long.MIN_VALUE, Long.MAX_VALUE,
        Collections.emptyList(), Collections.emptyList());
    // use LL shortlisting
    // result = binding.searchByExample(descriptor, null, 0, 10, -1, hmmScoring);
    logger.debug("result " + result);
    AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);

    // lulzsec
    // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "5104" }));

    // victim
    // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "727888" }));

    // lulzsec and thief
    // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "5104", "453733" }));

    // wikileaks
    // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "4547" }));

    // wikileaks, lulzsec
    List<String> ids = Arrays.asList("4547", "5104");
    descriptor = AvroUtils.createExemplarQuery(ids);

    // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "1", "12" }));
    // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "12", "616759" }));

    logger.debug("descriptor = " + AvroUtils.encodeJSON(descriptor));
    result = binding.searchByExample(descriptor, null, 0, 10, true, ids);
    AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);
    System.out.println("result = " + AvroUtils.encodeJSON((FL_PatternSearchResults) result));

    // query 0
    descriptor = AvroUtils.createExemplarQuery(Arrays.asList("505134", "137750", "146073", "28946", "11"));
    logger.debug("descriptor = " + AvroUtils.encodeJSON(descriptor));

    if (true) {
      return;
    }

    // FL_PatternDescriptor query = new FL_PatternDescriptor();
    // query.setUid("PD1");
    // query.setName("Pattern Descriptor 1");
    // query.setLinks(new ArrayList<FL_LinkMatchDescriptor>());
    //
    // List<String> exemplars;
    // List<FL_EntityMatchDescriptor> entityMatchDescriptors = new ArrayList<FL_EntityMatchDescriptor>();
    // exemplars = Arrays.asList("t1");
    //
    // FL_PropertyMatchDescriptor pmd1 = FL_PropertyMatchDescriptor.newBuilder()
    // .setKey(FL_PropertyTag.LABEL.toString()).setConstraint(FL_Constraint.EQUALS).setValue("skylar").build();
    //
    // FL_EntityMatchDescriptor t1 = FL_EntityMatchDescriptor.newBuilder().setUid("123").setRole("").setSameAs("")
    // .setEntities(Arrays.asList("t1")).build();
    //
    // FL_EntityMatchDescriptor p1 = new FL_EntityMatchDescriptor("P1", "Risky Partner", null, Arrays.asList("t1"),
    // null, null, exemplars, 1.0);
    // logger.debug("get entities " + p1.getEntities().size() + " : " + p1.getEntities().get(0));
    // entityMatchDescriptors.add(p1);
    // query.setEntities(entityMatchDescriptors);
    //
    // System.out.println("lender and partner query:");
    // System.out.println(AvroUtils.encodeJSON(query));
    // ResultInfo entities = binding.getEntitiesByID(p1);
    // logger.debug("Got " + entities);
    //
    // FL_PropertyMatchDescriptor p2 =
    // FL_PropertyMatchDescriptor.newBuilder().setKey(FL_PropertyTag.DATE.toString())
    // .setConstraint(FL_Constraint.GREATER_THAN).setValue("2013-01-01 01:01:01").build();
    //
    // FL_PropertyMatchDescriptor p4 =
    // FL_PropertyMatchDescriptor.newBuilder().setKey(FL_PropertyTag.DATE.toString())
    // .setConstraint(FL_Constraint.LESS_THAN).setValue("2013-04-05 01:01:01").build();
    //
    // FL_PropertyMatchDescriptor p3 = FL_PropertyMatchDescriptor.newBuilder().setKey(FL_PropertyTag.ID.toString())
    // .setConstraint(FL_Constraint.EQUALS).setValue("1").build();
    //
    // // this finds self loops IF both source and target are marked with id...
    //
    // Collection<ResultInfo> entities2 = binding.getEntitiesMatchingProperties(Arrays.asList(p3, p2), 10);
    // logger.debug("got " + entities2.size() + " : ");
    // for (ResultInfo r : entities2) {
    // logger.debug("\t" + r);
    // for (Map<String, String> row : r.rows) {
    // logger.debug("\t\t" + row);
    // }
    // }
    //
    // Collection<ResultInfo> entities3 = binding.getEntitiesMatchingProperties(Arrays.asList(p3, p2, p4), 100);
    // logger.debug("got " + entities3.size() + " : ");
    // for (ResultInfo r : entities3) {
    // logger.debug("\t" + r);
    // for (Map<String, String> row : r.rows) {
    // logger.debug("\t\t" + row);
    // }
    // }

  }

	/*    private static void testCombine() {
        long f = 10000;
        long s = 20000;

        BitcoinBinding b = new BitcoinBinding();
        logger.debug("both " + f + " " + s);
        long l = b.storeTwo(f, s);
        long[] both = b.recover(l);
        logger.debug("l " + l + " both " + both[0] + " " + both[1]);
        long ll = b.storeTwo(s, f);
        long[] both2 = b.recover(ll);
        logger.debug("l " + ll + " both " + both2[0] + " " + both2[1]);

        f = Integer.MAX_VALUE - 10;
        s = Integer.MAX_VALUE - 20;
        logger.debug("both " + f + " " + s);

        l = b.storeTwo(f, s);
        both = b.recover(l);
        logger.debug("l " + l + " both " + both[0] + " " + both[1]);
        ll = b.storeTwo(s, f);
        both2 = b.recover(ll);
        logger.debug("l " + ll + " both " + both2[0] + " " + both2[1]);
    }*/
}
