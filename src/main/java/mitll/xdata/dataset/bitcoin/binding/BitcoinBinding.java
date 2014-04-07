package mitll.xdata.dataset.bitcoin.binding;

import influent.idl.FL_EntityMatchDescriptor;
import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_Link;
import influent.idl.FL_LinkMatchResult;
import influent.idl.FL_LinkTag;
import influent.idl.FL_PatternDescriptor;
import influent.idl.FL_PatternSearchResult;
import influent.idl.FL_PatternSearchResults;
import influent.idl.FL_Property;
import influent.idl.FL_PropertyTag;
import influent.idl.FL_PropertyType;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mitll.xdata.AvroUtils;
import mitll.xdata.NodeSimilaritySearch;
import mitll.xdata.binding.Binding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.hmm.VectorObservation;
import mitll.xdata.scoring.Transaction;

import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA. User: go22670 Date: 7/10/13 Time: 4:08 PM To change this template use File | Settings |
 * File Templates.
 */
public class BitcoinBinding extends Binding {
    private static final Logger logger = Logger.getLogger(BitcoinBinding.class);
    private static final String TRANSACTIONS = "transactions";
	private static final int BUCKET_SIZE = 2;

    private NodeSimilaritySearch userIndex;

    private PreparedStatement pairConnectedStatement;
    private PreparedStatement edgeMetadataKeyStatement;
    private boolean useFastBitcoinConnectedTest = true;
    private final Map<Integer, Set<Integer>> stot = new HashMap<Integer, Set<Integer>>();

  public BitcoinBinding(DBConnection connection) {
    this(connection,true);
  }
    /**
     * @param connection
     * @param useFastBitcoinConnectedTest
     * @throws Exception
     */
    private BitcoinBinding(DBConnection connection, boolean useFastBitcoinConnectedTest) {
        super(connection);
        this.useFastBitcoinConnectedTest = useFastBitcoinConnectedTest;
        prefixToTable.put("t", TRANSACTIONS);
        tableToPrimaryKey.put(TRANSACTIONS, "SOURCE");

        for (String col : tableToPrimaryKey.values())
            addTagToColumn(FL_PropertyTag.ID, col);
        // addTagToColumn(FL_PropertyTag.ID, "TARGET");
        addTagToColumn(FL_PropertyTag.DATE, "TIME");
        addTagToColumn(FL_PropertyTag.AMOUNT, "AMOUNT");
        tableToDisplay.put(TRANSACTIONS, "transactions");
        Collection<String> tablesToQuery = prefixToTable.values();
        tablesToQuery = new ArrayList<String>(tablesToQuery);

        populateTableToColumns(connection, tablesToQuery, connection.getType());
        populateColumnToTables();

        // logger.debug("cols " + tableToColumns);

      try {
        InputStream userIds = this.getClass().getResourceAsStream("/bitcoin_feats_tsv/bitcoin_ids.tsv");
        InputStream userFeatures = this.getClass().getResourceAsStream(
                "/bitcoin_feats_tsv/bitcoin_features_standardized.tsv");
        logger.debug("indexing node features");
        userIndex = new NodeSimilaritySearch(userIds, userFeatures);
        logger.debug("done indexing node features");

        // create prepared statement for determining if two nodes connected
        StringBuilder sql = new StringBuilder();
        sql.append("select count(1) from (");
        sql.append(" select 1");
        sql.append(" from transactions");
        sql.append(" where (source= ? and target = ?)");
        sql.append(" limit 1");
        sql.append(" ) as temp");
        pairConnectedStatement = connection.getConnection().prepareStatement(sql.toString());

        sql = new StringBuilder();
        sql.append("select * from (");
        sql.append(" select transid");
        sql.append(" from transactions");
        sql.append(" where (source = ? and target = ?)");
        sql.append(" limit 1");
        sql.append(" ) as temp");
        edgeMetadataKeyStatement = connection.getConnection().prepareStatement(sql.toString());

        if (useFastBitcoinConnectedTest) {
            populateInMemoryAdjacency();
        }
      } catch (Exception e) {
        logger.error("got " +e,e);
      }
    }

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
            int target = (int) recover[1];
            Set<Integer> integers = stot.get(source);
            if (integers == null)
                stot.put(source, integers = new HashSet<Integer>());
            if (!integers.contains(target))
                integers.add(target);

            if (count % 1000000 == 0) {
                logger.debug("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count + " ms/read");
            }
        }
        br.close();
        logger.debug("populateInMemoryAdjacency : took  " + (System.currentTimeMillis() - then) + " millis to read "
                + count + " entries.");

    }

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
     * @paramx connection
     * @throws SQLException
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

    private long storeTwo(long low, long high) {
        long combined = low;
        combined += high << 32;
        return combined;
    }

    private long[] recover(long combined) {
        long low = combined & Integer.MAX_VALUE;
        long high = (combined >> 32) & Integer.MAX_VALUE;
        long[] both = new long[2];

        both[0] = low;
        both[1] = high;
        return both;
    }

    /**
     * @see #searchByExample(influent.idl.FL_PatternDescriptor, String, long, long)
     * @param id
     * @param k
     * @param skipSelf
     * @return
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
     * @see #searchByExample(influent.idl.FL_PatternDescriptor, String, long, long)
     * @param id1
     * @param id2
     * @return
     */
    @Override
    protected double getSimilarity(String id1, String id2) {
        return userIndex.similarity(id1, id2);
    }

    /**
     * @see #connectedGroup(java.util.List)
     * @param i
     * @param j
     * @return
     * @throws Exception
     */
    @Override
    protected boolean isPairConnected(String i, String j) throws Exception {
        if (useFastBitcoinConnectedTest)
            return inMemoryIsPairConnected(i, j);

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

    boolean inMemoryIsPairConnected(String i, String j) {
        int f = Integer.parseInt(i);
        int s = Integer.parseInt(j);

        Set<Integer> integers = stot.get(f);
        if (integers != null && integers.contains(s))
            return true;

        Set<Integer> integers2 = stot.get(s);
      return integers2 != null && integers2.contains(f);
    }

    /**
     * @see #getLinks(java.util.List)
     * @param id1
     * @param id2
     * @return
     * @throws Exception
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
        return createProperty("transid", Long.parseLong(id), FL_PropertyType.LONG);
    }

    @Override
    protected List<Edge> getAllLinks(List<String> ids) {
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
                        edges.add((Edge) edge);
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
                        edges.add((Edge) edge);
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
     * @see #rescoreWithHMM
     * @param edges
     * @return
     */
    @Override
    protected List<Transaction> createFeatureVectors(List<Edge> edges, List<String> exemplarIDs) {
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
    }
    
	@Override
	protected List<VectorObservation> createObservationVectors(List<Edge> edges, List<String> ids) {
		// sort edges
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

		//
		// make one observation per temporal bucket
		//

		List<VectorObservation> observations = new ArrayList<VectorObservation>();

		// map all pairs of query nodes to indices (that occur after initial fixed part of feature vector)
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

		logger.debug("indexMap = " + indexMap);

		BitcoinEdge firstEdge = (BitcoinEdge) edges.get(0);
		int edgeCount = 0;
		long lastTime = firstEdge.getTime();
		long lastBucketFirstTime = firstEdge.getTime();
		boolean newBucket = true;
		long thisBucketFirstTime = -1;

		double[] features = new double[2 + (numNodes * (numNodes - 1)) / 2];
		List<Edge> bucketEdges = new ArrayList<Edge>();

		for (int i = 0; i < edges.size(); i++) {
			edgeCount++;
			BitcoinEdge edge = (BitcoinEdge) edges.get(i);
			bucketEdges.add(edge);

			long thisTime = edge.getTime();

			// accumulate mean time between transactions
			if (!newBucket) {
				features[0] += (thisTime - lastTime);
			}

			// time since last bucket (will be zero for very first bucket)
			if (newBucket) {
				features[1] = (thisTime - lastTime);
				thisBucketFirstTime = thisTime;
			}

			newBucket = false;
			lastTime = thisTime;

			// net flow between pairs of nodes
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

			if (edgeCount == BUCKET_SIZE) {
				// normalize mean time between transactions by bucket length
				features[0] /= (lastTime - thisBucketFirstTime);

				// normalize time since last bucket by total length spanned by both buckets
				// Note: features[1] is already zero for very first bucket, so this doesn't do anything
				features[1] /= (lastTime - lastBucketFirstTime);

				// add observation
				VectorObservation observation = new VectorObservation(features);
				observation.setEdges(bucketEdges);
				observations.add(observation);

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
			}

			// normalize time since last bucket by total length spanned by both buckets
			// Note: features[1] is already zero for very first bucket, so this doesn't do anything
			features[1] /= (lastTime - lastBucketFirstTime);

			// add observation
			VectorObservation observation = new VectorObservation(features);
			observation.setEdges(bucketEdges);
			observations.add(observation);
		}

		return observations;
	}

    /**
     * @see #searchByExample
     * @param example
     * @param result
     * @param edges
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
            link.setTags(new ArrayList<FL_LinkTag>());
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

    public static class BitcoinEdge implements Edge/* , Comparable<BitcoinEdge> */{
        private int source;
        private int target;
        private long time;
        private double amount;
        private double usd;

        /** deviation from population mean usd */
        private double deviationFromPopulation;

        /** deviation from target's mean usd */
        private double deviationFromOwnCredits;

        /** deviation from source's mean usd */
        private double deviationFromOwnDebits;

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
        public Object getSource() {
            return source;
        }

        @Override
        public Object getTarget() {
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

    public static void main(String[] arg) throws Exception {
        System.getProperties().put("logging.properties", "log4j.properties");

        // testCombine();
        BitcoinBinding binding = null;
        try {
            // binding = new BitcoinBinding(new H2Connection("bitcoin"));
            binding = new BitcoinBinding(new H2Connection("c:/temp/bitcoin", "bitcoin"), true);
            // binding = new BitcoinBinding(new H2Connection( "bitcoin"), false);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        logger.debug("got " + binding);

        // List<String> ids = Arrays.asList(new String[] { "505134", "137750", "146073", "28946", "11" });
        // List<Edge> edges = binding.getAllLinks(ids);

        // List<FL_PatternSearchResult> results = binding.getShortlistAptima(0);
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
        descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "11", "1598539", "988143" }));

        // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "505134", "137750", "146073",
        // "28946",
        // "11" }));

        boolean hmmScoring = true;
        // use aptima precomputed results
        logger.debug("descriptor = " + AvroUtils.encodeJSON(descriptor));
        result = binding.searchByExample(descriptor, null, 0, 100, hmmScoring, Long.MIN_VALUE, Long.MAX_VALUE);
        // use LL shortlisting
        // result = binding.searchByExample(descriptor, null, 0, 10, -1, hmmScoring);
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
        descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "4547", "5104" }));

        // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "1", "12" }));
        // descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "12", "616759" }));

        logger.debug("descriptor = " + AvroUtils.encodeJSON(descriptor));
        result = binding.searchByExample(descriptor, null, 0, 10);
        AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);
        System.out.println("result = " + AvroUtils.encodeJSON((FL_PatternSearchResults) result));

        // query 0
        descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "505134", "137750", "146073", "28946", "11" }));
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
