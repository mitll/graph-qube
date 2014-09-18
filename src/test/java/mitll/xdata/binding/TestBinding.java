package mitll.xdata.binding;

import influent.idl.FL_EntityMatchDescriptor;
import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_Link;
import influent.idl.FL_LinkMatchResult;
import influent.idl.FL_LinkTag;
import influent.idl.FL_PatternDescriptor;
import influent.idl.FL_PatternSearchResult;
import influent.idl.FL_PatternSearchResults;
import influent.idl.FL_Property;
import influent.idl.FL_PropertyType;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mitll.xdata.AvroUtils;
import mitll.xdata.hmm.VectorObservation;
import mitll.xdata.scoring.Transaction;

import org.apache.log4j.Logger;

public class TestBinding extends Binding {
	private static Logger logger = Logger.getLogger(TestBinding.class);

	public static int BUCKET_SIZE = 2;

	private List<TestEdge> testEdges;
	private Map<String, List<String>> neighbors;

	public TestBinding() {
		super();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

		testEdges = new ArrayList<TestEdge>();
		try {
			// a-b edges outside time range
			testEdges.add(new TestEdge("a", "b", sdf.parse("2012-10-15 12:00").getTime(), 5000.00, 0, 0, 0));
			testEdges.add(new TestEdge("b", "a", sdf.parse("2012-11-12 13:00").getTime(), 3000.00, 0, 0, 0));

			// fan out
			testEdges.add(new TestEdge("a", "b", sdf.parse("2013-10-15 14:00").getTime(), 1000.00, 0, 0, 0));
			testEdges.add(new TestEdge("a", "c", sdf.parse("2013-10-16 15:00").getTime(), 800.00, 0, 0, 0));
			// fan in
			testEdges.add(new TestEdge("b", "d", sdf.parse("2013-10-17 16:00").getTime(), 950.00, 0, 0, 0));
			testEdges.add(new TestEdge("c", "d", sdf.parse("2013-10-18 17:00").getTime(), 750.00, 0, 0, 0));

			// fan out
			testEdges.add(new TestEdge("e", "f", sdf.parse("2013-09-15 18:00").getTime(), 1200.00, 0, 0, 0));
			testEdges.add(new TestEdge("e", "g", sdf.parse("2013-09-17 19:00").getTime(), 1300.00, 0, 0, 0));
			// fan in
			testEdges.add(new TestEdge("f", "h", sdf.parse("2013-09-18 20:00").getTime(), 1100.00, 0, 0, 0));
			testEdges.add(new TestEdge("g", "h", sdf.parse("2013-09-20 21:00").getTime(), 1250.00, 0, 0, 0));

			// extra edges before and after this matching fan-out-fan-in
			testEdges.add(new TestEdge("e", "h", sdf.parse("2013-9-01 12:00").getTime(), 600.00, 0, 0, 0));
			testEdges.add(new TestEdge("h", "e", sdf.parse("2013-9-02 13:00").getTime(), 500.00, 0, 0, 0));
			testEdges.add(new TestEdge("f", "g", sdf.parse("2013-9-03 12:00").getTime(), 400.00, 0, 0, 0));
			testEdges.add(new TestEdge("g", "f", sdf.parse("2013-9-04 14:00").getTime(), 350.00, 0, 0, 0));

			testEdges.add(new TestEdge("f", "g", sdf.parse("2013-9-22 12:00").getTime(), 600.00, 0, 0, 0));
			testEdges.add(new TestEdge("f", "g", sdf.parse("2013-9-23 13:00").getTime(), 1000.00, 0, 0, 0));
			testEdges.add(new TestEdge("g", "f", sdf.parse("2013-9-24 15:00").getTime(), 800.00, 0, 0, 0));

			// cycle
			testEdges.add(new TestEdge("i", "j", sdf.parse("2013-08-15 12:00").getTime(), 1000.00, 0, 0, 0));
			testEdges.add(new TestEdge("j", "k", sdf.parse("2013-08-16 13:00").getTime(), 1200.00, 0, 0, 0));
			testEdges.add(new TestEdge("k", "l", sdf.parse("2013-08-17 14:00").getTime(), 1000.00, 0, 0, 0));
			testEdges.add(new TestEdge("l", "i", sdf.parse("2013-08-18 15:00").getTime(), 1300.00, 0, 0, 0));

			// extra edges before and after cycle
			testEdges.add(new TestEdge("i", "j", sdf.parse("2013-08-01 12:00").getTime(), 1100.00, 0, 0, 0));
			testEdges.add(new TestEdge("j", "k", sdf.parse("2013-08-02 12:00").getTime(), 1200.00, 0, 0, 0));
			testEdges.add(new TestEdge("k", "l", sdf.parse("2013-08-03 12:00").getTime(), 1300.00, 0, 0, 0));
			testEdges.add(new TestEdge("l", "i", sdf.parse("2013-08-04 12:00").getTime(), 1400.00, 0, 0, 0));

			// random
			testEdges.add(new TestEdge("m", "a", sdf.parse("2013-07-15 12:00").getTime(), 200.00, 0, 0, 0));
			testEdges.add(new TestEdge("n", "b", sdf.parse("2013-07-16 12:00").getTime(), 300.00, 0, 0, 0));
			testEdges.add(new TestEdge("o", "e", sdf.parse("2013-07-17 12:00").getTime(), 200.00, 0, 0, 0));
			testEdges.add(new TestEdge("p", "h", sdf.parse("2013-07-18 12:00").getTime(), 400.00, 0, 0, 0));
			testEdges.add(new TestEdge("c", "q", sdf.parse("2013-07-19 12:00").getTime(), 300.00, 0, 0, 0));
			testEdges.add(new TestEdge("d", "r", sdf.parse("2013-07-20 12:00").getTime(), 200.00, 0, 0, 0));
			testEdges.add(new TestEdge("f", "s", sdf.parse("2013-07-21 12:00").getTime(), 100.00, 0, 0, 0));
			testEdges.add(new TestEdge("g", "t", sdf.parse("2013-07-22 12:00").getTime(), 500.00, 0, 0, 0));
		} catch (ParseException e) {
			e.printStackTrace();
		}

		// compute devFromPop, etc.?

		// nearest neighbors
		neighbors = new HashMap<String, List<String>>();
		neighbors.put("a", Arrays.asList(new String[] { "e", "i","a" }));
		neighbors.put("b", Arrays.asList(new String[] { "f", "j","b" }));
		neighbors.put("c", Arrays.asList(new String[] { "g", "l","c" }));
		neighbors.put("d", Arrays.asList(new String[] { "h", "k","d" }));

    testCreateObservationVectors();

  }

	@Override
	protected List<String> getNearestNeighbors(String id, int k, boolean skipSelf) {
		if (neighbors.containsKey(id)) {
			return neighbors.get(id);
		}
		return new ArrayList<String>();
	}

	@Override
	protected double getSimilarity(String id1, String id2) {
		if (id1.equals("a") && id2 == "e") {
			return 0.7;
		}
		return 0.8;
	}

	@Override
	protected List<FL_LinkMatchResult> createAggregateLinks(FL_PatternDescriptor example,
			FL_PatternSearchResult result, List<Edge> edges) {
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
			TestEdge edge = (TestEdge) temp;
			String source = "" + edge.getSource();
			String target = "" + edge.getTarget();
			double amount = edge.getAmount();
			int sourcePosition = resultIdToQueryIdPosition.get(source);
			int targetPosition = resultIdToQueryIdPosition.get(target);
			String key;
			double out;
			double in;
			if (sourcePosition < targetPosition) {
				// edge 2 --> 1, key 1:2, so flow is "out" (from source of key)
				key = sourcePosition + ":" + targetPosition;
				out = amount;
				in = 0.0;
			} else {
				// edge 2 --> 1, key 1:2, so flow is "in" (to source of key)
				key = targetPosition + ":" + sourcePosition;
				out = 0.0;
				in = amount;
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

	@Override
	protected boolean isPairConnected(String id1, String id2) throws Exception {
		for (TestEdge edge : testEdges) {
			if (edge.getSource().equals(id1) && edge.getTarget().equals(id2)) {
				return true;
			}
			if (edge.getSource().equals(id2) && edge.getTarget().equals(id1)) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected String getEdgeMetadataKey(String id1, String id2) throws Exception {
		return null;
	}

	@Override
	protected FL_Property createEdgeMetadataKeyProperty(String id) {
		return null;
	}

	@Override
	protected List<Edge> getAllLinks(List<String> ids) {
		List<Edge> edges = new ArrayList<Edge>();
		Set<String> set = new HashSet<String>(ids);
		for (TestEdge edge : testEdges) {
			String source = (String) edge.getSource();
			String target = (String) edge.getTarget();
			long t = edge.getTime();
			if (set.contains(source) && set.contains(target)) {
				// make copy of edge?
				edges.add(edge);
			}
		}
		return edges;
	}

	@Override
	protected List<Edge> getAllLinks(List<String> ids, long startTime, long endTime) {
		List<Edge> edges = new ArrayList<Edge>();
		Set<String> set = new HashSet<String>(ids);
		for (TestEdge edge : testEdges) {
			String source = edge.getSource();
			String target = edge.getTarget();
			long t = edge.getTime();
			if (set.contains(source) && set.contains(target) && t >= startTime && t <= endTime) {
				// make copy of edge?
				edges.add(edge);
			}
		}
		return edges;
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

		TestEdge firstEdge = (TestEdge) edges.get(0);
		int edgeCount = 0;
		long lastTime = firstEdge.getTime();
		long lastBucketFirstTime = firstEdge.getTime();
		boolean newBucket = true;
		long thisBucketFirstTime = -1;

		double[] features = new double[2 + (numNodes * (numNodes - 1)) / 2];
		List<Edge> bucketEdges = new ArrayList<Edge>();

		for (int i = 0; i < edges.size(); i++) {
			edgeCount++;
			TestEdge edge = (TestEdge) edges.get(i);
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
				features[indexMap.get(key)] += edge.getAmount();
			} else {
				// this index is for net flow from target to source (so subtract amount)
				key = edge.getTarget() + ":" + edge.getSource();
				features[indexMap.get(key)] -= edge.getAmount();
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

	private void testCreateObservationVectors() {
		logger.debug("ENTER testCreateObservationVectors()");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

		// List<String> ids = Arrays.asList(new String[] { "a", "b", "c", "d" });
		List<String> ids = Arrays.asList(new String[] { "b", "a", "c", "d" });
		// List<Edge> edges = getAllLinks(ids, Long.MIN_VALUE, Long.MAX_VALUE);
    long start = 0;
    long end = 0;
    try {
      start = sdf.parse("2013-10-15 00:00").getTime();
      end = sdf.parse("2013-10-16 23:59").getTime();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    List<Edge> edges = getAllLinks(ids, start, end);
		for (Edge edge : edges) {
			logger.debug(edge);
		}
		List<VectorObservation> observations = createObservationVectors(edges, ids);
		for (VectorObservation observation : observations) {
			logger.debug(Arrays.toString(observation.getValues()));
		}
		logger.debug("EXIT testCreateObservationVectors()");
	}

	public static void main(String[] args) throws Exception {
		TestBinding binding = new TestBinding();

		FL_PatternDescriptor descriptor;
		Object result;
		long startTime;
		long endTime;
		boolean hmmScoring;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

		// entity search
		logger.debug("----------------------------------------------------------------");
		logger.debug("entity search");
		startTime = Long.MIN_VALUE;
		endTime = Long.MAX_VALUE;
		hmmScoring = false;
		descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "a" }));
		result = binding.searchByExample(descriptor, 0, 100, hmmScoring, startTime, endTime);
		logger.debug("result = " + result);
		AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);

		// 2-node pattern search
		logger.debug("----------------------------------------------------------------");
		logger.debug("2-node pattern search");
		startTime = Long.MIN_VALUE;
		endTime = Long.MAX_VALUE;
		hmmScoring = false;
		descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "a", "b" }));
		result = binding.searchByExample(descriptor, 0, 100, hmmScoring, startTime, endTime);
		logger.debug("result = " + result);
		AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);

		// 2-node pattern search w/restricted query time
		logger.debug("----------------------------------------------------------------");
		logger.debug("2-node pattern search w/restricted query time");
		// startTime = sdf.parse("2012-10-01 00:00").getTime();
		// endTime = sdf.parse("2012-12-01 00:00").getTime();
		startTime = sdf.parse("2013-10-01 00:00").getTime();
		endTime = sdf.parse("2013-11-01 00:00").getTime();
		hmmScoring = false;
		descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "b", "a" }));
		result = binding.searchByExample(descriptor, 0, 100, hmmScoring, startTime, endTime);
		logger.debug("result = " + result);
		AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);

		// fan-in-fan-out pattern search w/restricted query time
		logger.debug("----------------------------------------------------------------");
		logger.debug("2-node pattern search w/restricted query time");
		// startTime = sdf.parse("2012-10-01 00:00").getTime();
		// endTime = sdf.parse("2012-12-01 00:00").getTime();
		startTime = sdf.parse("2013-10-01 00:00").getTime();
		endTime = sdf.parse("2013-11-01 00:00").getTime();
		hmmScoring = true;
		descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "b", "a", "c", "d" }));
		result = binding.searchByExample(descriptor, 0, 100, hmmScoring, startTime, endTime);
		logger.debug("result = " + result);
		AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);
		
		logger.debug("((FL_PatternSearchResults) result).getResults().get(0).getClass() = " + ((FL_PatternSearchResults) result).getResults().get(0).getClass());
		PatternSearchResultWithState resultWithState = (PatternSearchResultWithState) ((FL_PatternSearchResults) result).getResults().get(0);
		logger.debug("resultWithState.getStates().size() = " + resultWithState.getStates().size());
		logger.debug("resultWithState.getPhaseLinks().size() = " + resultWithState.getPhaseLinks().size());
		logger.debug("resultWithState.getStates() = " + resultWithState.getStates());
		logger.debug("resultWithState.getPhaseLinks().get(0) = " + resultWithState.getPhaseLinks().get(0));
		logger.debug("resultWithState.getPhaseLinks().get(1) = " + resultWithState.getPhaseLinks().get(1));
	}

	@Override
	protected HashMap<String, String> getEdgeAttributes(String src,
			String dest, Set<String> columnNames) {
		// TODO Auto-generated method stub
		return null;
	}
}
