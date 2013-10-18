package mitll.xdata.binding;

import influent.idl.FL_Constraint;
import influent.idl.FL_EntityMatchDescriptor;
import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_Link;
import influent.idl.FL_LinkMatchDescriptor;
import influent.idl.FL_LinkMatchResult;
import influent.idl.FL_LinkTag;
import influent.idl.FL_PatternDescriptor;
import influent.idl.FL_PatternSearchResult;
import influent.idl.FL_PatternSearchResults;
import influent.idl.FL_Property;
import influent.idl.FL_PropertyMatchDescriptor;
import influent.idl.FL_PropertyTag;
import influent.idl.FL_PropertyType;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import mitll.xdata.AvroUtils;
import mitll.xdata.NodeSimilaritySearch;
import mitll.xdata.PrioritizedCartesianProduct;
import mitll.xdata.binding.Binding.Edge;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.db.MysqlConnection;
import mitll.xdata.graph.Graph;
import mitll.xdata.graph.GraphQuery;
import mitll.xdata.hmm.VectorObservation;
import mitll.xdata.scoring.Transaction;

import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA. User: go22670 Date: 6/25/13 Time: 4:23 PM To change this template use File | Settings |
 * File Templates.
 */
public class KivaBinding extends Binding {
    private static Logger logger = Logger.getLogger(KivaBinding.class);

    public static final String LENDERS = "lenders";
    public static final String PARTNERS_META_DATA = "partners_meta_data";
    public static final String LOAN_BORROWERS = "loan_borrowers";
    public static final String TEAMS = "teams";
    public static final String LENDER_LOAN_LINKS = "lender_loan_links";
    public static final String LENDER_TEAM_LINKS = "lender_team_links";
    // public static final String LENDER_TEAM_LINKS = "lender_team_links";
    public static final String LOAN_META_DATA = "loan_meta_data";

    // Collection<String> linkTables = new ArrayList<String>();

    NodeSimilaritySearch partnerIndex;
    NodeSimilaritySearch lenderIndex;
    
    PreparedStatement pairConnectedStatement;
    PreparedStatement edgeMetadataKeyStatement;

  public KivaBinding(DBConnection connection) throws Exception {
        super(connection);
        // so it we go from entity type to table by id prefix...

        // surely there's a better way

        /**
         * 
         lenders - 'l'+lenders_lenderId partners - 'p'+partners_id borrowers/loans - 'b'+loans_id brokers
         * 'p'+partners_id+'-'+loans_id
         */
        prefixToTable.put("l", LENDERS);
        prefixToTable.put("p", PARTNERS_META_DATA);
        // prefixToTable.put("b", LOAN_BORROWERS);
        // prefixToTable.put("t", TEAMS);

        tableToPrimaryKey.put(LENDERS, "lender_id");
        tableToPrimaryKey.put(PARTNERS_META_DATA, "partner_id");
        tableToPrimaryKey.put(TEAMS, "team_id");
        // tableToPrimaryKey.put(LOAN_BORROWERS, "borrower_id");
        tableToPrimaryKey.put(LOAN_META_DATA, "loan_id");

        for (String col : tableToPrimaryKey.values())
            addTagToColumn(FL_PropertyTag.ID, col);
        addTagToColumn(FL_PropertyTag.LABEL, "name");
        addTagToColumn(FL_PropertyTag.DATE, "member_since");
        addTagToColumn(FL_PropertyTag.DATE, "team_since");
        addTagToColumn(FL_PropertyTag.DATE, "start_date");
        addTagToColumn(FL_PropertyTag.GEO, "whereabouts");

        tableToPrimaryKey.put(LOAN_META_DATA, "loan_id");

        tableToDisplay.put(LENDERS, "lender");
        tableToDisplay.put(PARTNERS_META_DATA, "partner");
        tableToDisplay.put(TEAMS, "team");
        tableToDisplay.put(LOAN_META_DATA, "loan");
        // tableToDisplay.put(LOAN_BORROWERS, "borrower");

        Collection<String> tablesToQuery = prefixToTable.values();
        tablesToQuery = new ArrayList<String>(tablesToQuery);

        // TODO : add links tables...
        // tablesToQuery.add(LENDER_LOAN_LINKS);
        tablesToQuery.add(LOAN_META_DATA);

        populateTableToColumns(connection, tablesToQuery, connection.getType());

        populateColumnToTables();

        // linkTables.add(LENDER_LOAN_LINKS);
        // linkTables.add(LENDER_TEAM_LINKS);

        // to go from lender to loan :
        // lender->loan
        addLinkTable(LENDERS, LOAN_META_DATA, LENDER_LOAN_LINKS, "lender_id", "loan_id");
        addLinkTable(LOAN_META_DATA, LENDERS, LENDER_LOAN_LINKS, "loan_id", "lender_id");

        addLinkTable(LOAN_META_DATA, LOAN_BORROWERS, LOAN_BORROWERS, "loan_id", "loan_id");

        addLinkTable(PARTNERS_META_DATA, LOAN_META_DATA, LOAN_META_DATA, "partner_id", "loan_id");

        // lender -> team
        addLinkTable(LENDERS, TEAMS, LENDER_TEAM_LINKS, "lender_id", "team_id");
        addLinkTable(TEAMS, LENDERS, LENDER_TEAM_LINKS, "team_id", "lender_id");

        // initialize indexes for node similarity search

        InputStream partnerIds = this.getClass().getResourceAsStream("/kiva_feats_tsv/partner_ids.tsv");
        InputStream lenderIds = this.getClass().getResourceAsStream("/kiva_feats_tsv/lender_ids.tsv");

        InputStream partnerFeatures = this.getClass().getResourceAsStream(
                "/kiva_feats_tsv/partner_features_standardized.tsv");
        InputStream lenderFeatures = this.getClass().getResourceAsStream(
                "/kiva_feats_tsv/lender_features_standardized.tsv");

        /*
         * resourceAsStream = this.getClass().getResourceAsStream("kiva_feats_tsv/partner_ids.tsv");
         * logger.debug("resource " + resourceAsStream);
         * 
         * resourceAsStream = this.getClass().getResourceAsStream("partner_ids.tsv"); logger.debug("resource " +
         * resourceAsStream);
         */
        logger.debug("indexing partner features");
        partnerIndex = new NodeSimilaritySearch(partnerIds, partnerFeatures);
        logger.debug("indexing lender features");
        lenderIndex = new NodeSimilaritySearch(lenderIds, lenderFeatures);
        logger.debug("done indexing node features");
        
        // create prepared statement for determining if two nodes connected
        StringBuilder sql = new StringBuilder();
        sql.append("select count(1) from (");
        sql.append(" select 1");
        sql.append(" from edge_index");
        sql.append(" where (source_id = ? and target_id = ?)");
        sql.append(" limit 1");
        sql.append(" ) as temp");
        pairConnectedStatement = connection.getConnection().prepareStatement(sql.toString());
        
        sql = new StringBuilder();
        sql.append("select * from (");
        sql.append(" select edge_metadata_id");
        sql.append(" from edge_index");
        sql.append(" where (source_id = ? and target_id = ?)");
        sql.append(" limit 1");
        sql.append(" ) as temp");
        edgeMetadataKeyStatement = connection.getConnection().prepareStatement(sql.toString());
    }

    private void addLinkTable(String source, String target, String link, String sourceJoin, String targetJoin) {

        Map<String, ForeignLink> targetToLink2 = sourceToTargetToLinkTable.get(source);

        if (targetToLink2 == null) {
            targetToLink2 = new HashMap<String, ForeignLink>();
            sourceToTargetToLinkTable.put(source, targetToLink2);
        }

        targetToLink2.put(target, new ForeignLink(link, sourceJoin, targetJoin));
    }

    /**
     * Just testing a common case...
     * 
     * @param loan
     * @return
     */
    public List<String> getBorrowersForLoan(String loan) {
        ResultInfo loans = getEntities(LOAN_BORROWERS, "loan_id", loan, Long.MAX_VALUE);
        List<String> ids = new ArrayList<String>();
        for (Map<String, String> l : loans.rows) {
            ids.add(l.get("borrower_id"));
        }
        return ids;
    }

    /**
     * Filter for a set of entities matching the entity search parameters, which could be entity ids or field
     * properties. The time window is a start->end window with sql timestamps. So for Kiva, this a set of lender nodes
     * linked to a set of borrower nodes, with loans (lenders->borrowers) or payments (borrowers->lenders) between them.
     * 
     * For other datasets, the same entity types may be on both ends of the links...
     * 
     * The graph points to FL_Entity and FL_Link objects
     * 
     * Here we take two graphs (lender->loan) and (loan->borrower) and knit them together.
     * 
     * Loan changes from entity to edge.
     * 
     * @param entitySearchParameters
     *            e.g. whereabouts = "San Francisco" or whereabouts = "Uganda"
     * @param start
     *            date >= constraint on the link parameter
     * @param end
     *            date < constraint on the link parameter
     * @param linkSearchParameters
     *            optional further constraints on the link parameter (e.g. loans > $20000)
     * @param limit
     * @return
     */
    public Graph getSubgraphForEntitiesInTimeRange(List<Triple> entitySearchParameters, String start, String end,
            List<Triple> linkSearchParameters, long limit) {
        // for Kiva : lender->loan, then loan->borrower, outgoing transaction
        // and lender->payment, payment-> borrower? incoming transaction

        if (false) {
            Graph graph = new Graph();

            ForeignLink foreignLink = sourceToTargetToLinkTable.get(LENDERS).get(LOAN_META_DATA);

            Collection<Triple> localValues = new HashSet<Triple>();
            Map<String, Graph.Node> valueToNode = new HashMap<String, Graph.Node>();

            // String key = tableToPrimaryKey.get(fromTable);
            // List<Triple> sourceSearchCriteria = Arrays.asList(new Triple(key, fromID));

            graph.addNodes(findSourceNodes(LENDERS, entitySearchParameters, limit, foreignLink, localValues,
                    valueToNode));

            Collection<Triple> linkLocalValues = new HashSet<Triple>();
            // Map<String, Graph.Edge> valueToEdge = new HashMap<String, Graph.Edge>();

            findLinks2(foreignLink, localValues,
            // valueToNode,
                    linkLocalValues,
                    // valueToEdge,
                    limit);

            Collection<Graph.Edge> targetEdges = getTargetEdges(LOAN_META_DATA, foreignLink, linkLocalValues,
                    linkSearchParameters, limit);
        }

        Triple startRange = new Triple("posted_date", start, ">=");
        Triple endRange = new Triple("posted_date", end, "<");

        List<Triple> loanConstraints = Arrays.asList(startRange, endRange);
        loanConstraints.addAll(linkSearchParameters);

        Graph oneHopGraph = getOneHopGraph(LENDERS, entitySearchParameters, Collections.EMPTY_LIST, LOAN_META_DATA,
                loanConstraints, limit);
        logger.debug("got " + oneHopGraph);
        List<String> loanIDs = new ArrayList<String>();
        List<Graph.Node> lenders = new ArrayList<Graph.Node>();
        for (Graph.Node n : oneHopGraph.getNodes()) {
            if (n.getType().equals(LOAN_META_DATA)) {
                loanIDs.add(n.getEntity().getUid());
                // idToLoan.put(n.getEntity().getUid())
            } else {
                lenders.add(n);
            }
        }

        Map<String, Graph.Edge> idToLoanEdge = new HashMap<String, Graph.Edge>();
        for (Graph.Node n : lenders) {
            List<Graph.Edge> edges = n.getEdges();
            // List<Graph.Node> loanNodes = new ArrayList<Graph.Node>();

            List<Graph.Edge> newEdges = new ArrayList<Graph.Edge>();

            for (Graph.Edge e : edges) {
                Graph.Node loan = e.getTarget();

                String uid = loan.getProps().get(tableToPrimaryKey.get(LOAN_META_DATA));
                Graph.Edge loanEdge = new Graph.Edge(uid, LOAN_META_DATA, loan.getProps());
                loanEdge.setSource(n);
                newEdges.add(loanEdge);
                idToLoanEdge.put(uid, loanEdge);
            }
            edges.clear();
            n.getEdges().addAll(newEdges);
        }

        Graph oneHopGraphLoanToBorrower = getOneHopGraph(LOAN_META_DATA, linkSearchParameters, Collections.EMPTY_LIST,
                LOAN_BORROWERS, entitySearchParameters, limit);

        // TODO : set target node on the loan edges
        // TODO : complicated by fact that there can be multiple borrowers for a loan, so we probably need to copy
        // loan edges coming from the lenders for as many borrowers as there are (e.g. )
        Map<String, Graph.Node> idToLoan = new HashMap<String, Graph.Node>();
        for (Graph.Node n : oneHopGraphLoanToBorrower.getNodes()) {
            if (n.getType().equals(LOAN_META_DATA)) {
                idToLoan.put(n.getEntity().getUid(), n);

                Graph.Edge edge = idToLoanEdge.get(n.getEntity().getUid());
                edge.setTarget(n);
            }
        }

        // graph.addNodes(targetEdges);

        return oneHopGraph;
    }

    public List<Graph> getSubgraphsForEntitiesInTimeRanges(List<Triple> entitySearchParameters,
            List<GraphQuery.TimeRange> timeRanges, List<Triple> linkSearchParameters, long limit) {
        return null;
    }

    /*
     * public static class TimeRange { String start, end; public TimeRange(String start, String end) { this.start =
     * start; this.end = end; } }
     */

    public Graph getOneHopGraph(String fromTable, String fromID, String toTable, long limit) {
        String key = tableToPrimaryKey.get(fromTable);

        return getOneHopGraph(fromTable, Arrays.asList(new Triple(key, fromID)), Collections.EMPTY_LIST, toTable,
                Collections.EMPTY_LIST, limit);
    }

    /**
     * Initially create a node-edge-node graph, and then later piece them together to make larger graph...
     * 
     * For example loans for lender, (lenders for loan?) or loan->lender Ultimately lender->loan->borrower
     * 
     * @param fromTable
     * @paramx fromID
     * @param toTable
     * @paramx toID
     * @return
     */
    public Graph getOneHopGraph(String fromTable, List<Triple> sourceEntitySearchCriteria,
            List<Triple> linkSearchCriteria, String toTable, List<Triple> targetEntitySearchCriteria, long limit) {
        // first - get source nodes
        Graph graph = new Graph();

        ForeignLink foreignLink = sourceToTargetToLinkTable.get(fromTable).get(toTable);

        Collection<Triple> localValues = new HashSet<Triple>();
        Map<String, Graph.Node> valueToNode = new HashMap<String, Graph.Node>();

        // String key = tableToPrimaryKey.get(fromTable);
        // List<Triple> sourceSearchCriteria = Arrays.asList(new Triple(key, fromID));

        graph.addNodes(findSourceNodes(fromTable, sourceEntitySearchCriteria, limit, foreignLink, localValues,
                valueToNode));

        // logger.debug("graph " + graph);
        // next, get the links

        Collection<Triple> linkLocalValues = new HashSet<Triple>();
        Map<String, Graph.Edge> valueToEdge = new HashMap<String, Graph.Edge>();

        findLinks(foreignLink, localValues, linkSearchCriteria, valueToNode, linkLocalValues, valueToEdge, limit);

        // if (true) return graph;

        // logger.debug("value -> edge " + valueToEdge);

        graph.addNodes(getTargetNodes(toTable, foreignLink, linkLocalValues, targetEntitySearchCriteria, valueToEdge,
                limit));
        return graph;
    }

    private Collection<Graph.Node> getTargetNodes(String toTable, ForeignLink foreignLink,
            Collection<Triple> linkLocalValues, Collection<Triple> targetSearchCriteria,
            Map<String, Graph.Edge> valueToEdge, long limit) {
        ResultInfo targetNodes = getEntities(toTable, linkLocalValues, targetSearchCriteria, limit);
        String key2 = tableToPrimaryKey.get(toTable);
        logger.debug("table " + toTable + " key2 " + key2 + " link local " + linkLocalValues + " result " + targetNodes);
        List<Graph.Node> targets = new ArrayList<Graph.Node>();
        for (Map<String, String> entity : targetNodes.rows) {
            String joinValue = entity.get(foreignLink.targetPair.entityKey);

            Graph.Node target = new Graph.Node(entity.get(key2), targetNodes.getTable(), entity);
            target.setEntity(makeEntity(targetNodes.nameToType, key2, entity));
            targets.add(target);

            Graph.Edge edge = valueToEdge.get(joinValue);
            target.addEdge(edge);
            edge.setTarget(target);
        }
        return targets;
    }

    private Collection<Graph.Edge> getTargetEdges(String toTable, ForeignLink foreignLink,
            Collection<Triple> linkLocalValues, Collection<Triple> linkSearchCriteria,
            // Map<String, Graph.Edge> valueToEdge,
            long limit) {
        ResultInfo targetEdges = getEntities(toTable, linkLocalValues, linkSearchCriteria, limit);
        String key2 = tableToPrimaryKey.get(toTable);
        logger.debug("table " + toTable + " key2 " + key2 + " link local " + linkLocalValues);// + " result " +
                                                                                              // targetNodes);
        List<Graph.Edge> targets = new ArrayList<Graph.Edge>();
        for (Map<String, String> entity : targetEdges.rows) {
            String joinValue = entity.get(foreignLink.targetPair.entityKey);

            Graph.Edge target = new Graph.Edge(entity.get(key2), targetEdges.getTable(), entity);
            target.setLink(makeLink(targetEdges.nameToType, key2, entity, "", ""));
            targets.add(target);

            /*
             * Graph.Edge edge = valueToEdge.get(joinValue); target.addEdge(edge); edge.setTarget(target);
             */
        }
        return targets;
    }

    private void findLinks(ForeignLink foreignLink, Collection<Triple> localValues,
            Collection<Triple> linkSearchCriteria,

            Map<String, Graph.Node> valueToNode, Collection<Triple> linkLocalValues,
            Map<String, Graph.Edge> valueToEdge, long limit) {
        ResultInfo links = getEntities(foreignLink.linkTable, localValues, linkSearchCriteria, limit);
        String table = links.getTable();
        // logger.debug("link table "+ table + " value->node " + valueToNode);

        for (Map<String, String> entity : links.rows) {
            // hook up the first nodes to the edges
            String joinValue = entity.get(foreignLink.sourcePair.foreignKey);
            Graph.Node sourceNode = valueToNode.get(joinValue);
            // logger.debug("source node for " + joinValue + " is " +sourceNode);

            Graph.Edge edge = new Graph.Edge(joinValue, table, entity);
            sourceNode.addEdge(edge);
            edge.setSource(sourceNode);
            edge.setLink(makeLink(links.nameToType, foreignLink.sourcePair.foreignKey, entity, sourceNode.getEntity()
                    .getUid(), ""));

            String joinValue2 = entity.get(foreignLink.targetPair.foreignKey);
            linkLocalValues.add(new Triple(foreignLink.targetPair.entityKey, joinValue2));
            valueToEdge.put(joinValue2, edge);
        }
    }

    private void findLinks2(ForeignLink foreignLink, Collection<Triple> localValues,
    // Map<String, Graph.Node> valueToNode,
            Collection<Triple> linkLocalValues,
            // Map<String, Graph.Edge> valueToEdge,
            long limit) {
        ResultInfo links = getOrEntities(foreignLink.linkTable, localValues, limit);
        String table = links.getTable();
        // logger.debug("link table "+ table + " value->node " + valueToNode);

        for (Map<String, String> entity : links.rows) {
            // hook up the first nodes to the edges
            /*
             * String joinValue = entity.get(foreignLink.sourcePair.foreignKey); Graph.Node sourceNode =
             * valueToNode.get(joinValue); // logger.debug("source node for " + joinValue + " is " +sourceNode);
             * 
             * Graph.Edge edge = new Graph.Edge(joinValue, table, entity); sourceNode.addEdge(edge);
             * edge.setSource(sourceNode);
             * edge.setLink(makeLink(links.nameToType,foreignLink.sourcePair.foreignKey,entity
             * ,sourceNode.getEntity().getUid(),""));
             */
            String joinValue2 = entity.get(foreignLink.targetPair.foreignKey);
            linkLocalValues.add(new Triple(foreignLink.targetPair.entityKey, joinValue2));
            // valueToEdge.put(joinValue2,edge);
        }
    }

    private Collection<Graph.Node> findSourceNodes(String fromTable, List<Triple> sourceSearchCriteria, long limit,
            ForeignLink foreignLink,

            Collection<Triple> localValues, Map<String, Graph.Node> valueToNode) {
        String key = tableToPrimaryKey.get(fromTable);
        ResultInfo sourceEntities = getEntities(fromTable, sourceSearchCriteria, limit);
        for (Map<String, String> row : sourceEntities.rows) {
            Graph.Node node = new Graph.Node(row.get(key), sourceEntities.getTable(), row);
            node.setEntity(makeEntity(sourceEntities.nameToType, key, row));

            String joinValue = row.get(foreignLink.sourcePair.entityKey);
            localValues.add(new Triple(foreignLink.sourcePair.foreignKey, joinValue));
            valueToNode.put(joinValue, node);
        }

        return valueToNode.values();
    }

    public List<LoanInfo> getLoanEdges() throws Exception {
        String sql = "select loan.loan_id loan_id, ln.lender_id lender_id, loan.partner_id partner_id from lenders ln, loan_meta_data loan,"
                + " lender_loan_links link where ln.lender_id = link.lender_id and link.loan_id = loan.loan_id";

        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet rs = statement.executeQuery();
        List<LoanInfo> loans = new ArrayList<LoanInfo>();
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        while (rs.next()) {
            // Map<String, String> row = getRow(rs, nameToType);
            loans.add(new LoanInfo(rs.getString(1), rs.getString(2), rs.getString(3)));
        }
        if (false) {
            logger.debug("Got " + rows.size() + " ");
            for (Map<String, String> row : rows)
                logger.debug(row);
        }
        rs.close();
        statement.close();
        return loans;
    }

    public static class LoanInfo {
        public String loan_id, lender_id, partner_id;

        public LoanInfo(String loan_id, String lender_id, String partner_id) {
            this.loan_id = loan_id;
            this.lender_id = lender_id;
            this.partner_id = partner_id;
        }
    }
    
    @Override
    protected List<String> getNearestNeighbors(String id, int k, boolean skipSelf) {
        List<String> neighbors = new ArrayList<String>();
        char type = id.charAt(0);
        if (type == 'l') {
            // lender
            neighbors = lenderIndex.neighbors(id, k);
        } else if (type == 'p') {
            // partner
            neighbors = partnerIndex.neighbors(id, k);
        }
        if (skipSelf && neighbors.size() >= 1 && neighbors.get(0).equals(id)) {
            neighbors.remove(0);
        }
       return neighbors;
    }

    @Override
    protected double getSimilarity(String id1, String id2) {
        double similarity = Double.MAX_VALUE;
        char type = id1.charAt(0);
        if (type == 'p') {
            similarity = partnerIndex.similarity(id1, id2);
        } else if (type == 'l') {
            similarity = lenderIndex.similarity(id1, id2);
        }
        return similarity;
    }

  /**
   * @see #connectedGroup(java.util.List)
   *
   * @param i
   * @param j
   * @return
   * @throws Exception
   */
    @Override
    protected boolean isPairConnected(String i, String j) throws Exception {
        int parameterIndex;
        ResultSet rs;
        // i is source
        parameterIndex = 1;
        pairConnectedStatement.setString(parameterIndex++, i);
        pairConnectedStatement.setString(parameterIndex++, j);
        rs = pairConnectedStatement.executeQuery();
        if (rs.next()) {
            if (rs.getInt(1) > 0) {
                return true;
            }
        }
        // i is target
        parameterIndex = 1;
        pairConnectedStatement.setString(parameterIndex++, j);
        pairConnectedStatement.setString(parameterIndex++, i);
        rs = pairConnectedStatement.executeQuery();
        if (rs.next()) {
            if (rs.getInt(1) > 0) {
                return true;
            }
        }
        return false;
    }

    private PreparedStatement getLinkPreparedStatement() throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append("select * from (");
        sql.append(" select edge_metadata_id");
        sql.append(" from edge_index");
        sql.append(" where (source_id = ? and target_id = ?)");
        sql.append(" or (target_id = ? and source_id = ?)");
        sql.append(" limit 1");
        sql.append(" ) as temp");
        return connection.prepareStatement(sql.toString());
    }
    
    @Override
    protected String getEdgeMetadataKey(String id1, String id2) throws Exception {
        int parameterIndex;
        ResultSet rs;
        // i as source
        parameterIndex = 1;
        edgeMetadataKeyStatement.setString(parameterIndex++, id1);
        edgeMetadataKeyStatement.setString(parameterIndex++, id2);
        rs = edgeMetadataKeyStatement.executeQuery();
        if (rs.next()) {
            return rs.getString(1);
        }
        // i as target
        parameterIndex = 1;
        edgeMetadataKeyStatement.setString(parameterIndex++, id2);
        edgeMetadataKeyStatement.setString(parameterIndex++, id1);
        rs = edgeMetadataKeyStatement.executeQuery();
        if (rs.next()) {
            return rs.getString(1);
        }
        return null;
    }
    
    @Override
    protected FL_Property createEdgeMetadataKeyProperty(String id) {
        return createProperty("loan_id", Long.parseLong(id), FL_PropertyType.LONG);
    }

    @Override
    protected List<Edge> getAllLinks(List<String> ids) {
        return null;
    }

    @Override
    protected List<Edge> getAllLinks(List<String> ids, long t0, long t1) {
        return null;
    }
    
    @Override
    protected List<Transaction> createFeatureVectors(List<Edge> edges, List<String> exemplarIDs) {
        return null;
    }
    
	@Override
	protected List<VectorObservation> createObservationVectors(List<Edge> edges, List<String> ids) {
		return null;
	}

    @Override
    protected List<FL_LinkMatchResult> createAggregateLinks(FL_PatternDescriptor example, FL_PatternSearchResult result, List<Edge> edges) {
    	return null;
    }
    
    private FL_Link makeLink(Map<String, String> colToType, String primaryKeyCol, Map<String, String> entityMap,
            String source, String target) {
        FL_Link link = new FL_Link();
        link.setTags(new ArrayList<FL_LinkTag>()); // TODO : none for now... financial?

        List<FL_Property> properties = new ArrayList<FL_Property>();
        link.setProperties(properties);

        String uid = setProperties(properties, colToType, primaryKeyCol, entityMap);

        // link.setUid(uid);

        link.setSource(source);
        link.setTarget(target);
        return link;
    }

    public static void main(String[] args) throws Exception {
        System.getProperties().put("logging.properties", "log4j.properties");
        // System.getProperties().put("log4j.configuration", "/log4j.properties");

        KivaBinding kivaBinding = null;

        DBConnection connection;
//        if (args.length == 3) {
//            // database, user, password
//            connection = new MysqlConnection(args[0], args[1], args[2]);
//        } else {
//            connection = new MysqlConnection("kiva");
//        }
        connection = new H2Connection("c:/temp/kiva", "kiva");
        try {
            kivaBinding = new KivaBinding(connection);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        logger.debug("got " + kivaBinding);
        System.out.println("got " + kivaBinding);

        FL_PatternDescriptor query = new FL_PatternDescriptor();
        query.setUid("PD1");
        query.setName("Pattern Descriptor 1");
        query.setLinks(new ArrayList<FL_LinkMatchDescriptor>());

        List<String> exemplars;
        List<FL_EntityMatchDescriptor> entityMatchDescriptors = new ArrayList<FL_EntityMatchDescriptor>();
        exemplars = Arrays.asList(new String[] { "l0376099" });
        entityMatchDescriptors.add(new FL_EntityMatchDescriptor("L1", "Unlucky Lender", null, null, null, exemplars, null));
        exemplars = Arrays.asList(new String[] { "p137" });
        entityMatchDescriptors.add(new FL_EntityMatchDescriptor("P1", "Risky Partner", null, null, null, exemplars, null));
        query.setEntities(entityMatchDescriptors);

        System.out.println("lender and partner query:");
        System.out.println(AvroUtils.encodeJSON(query));

        // http://localhost:4567/pattern/search/example?example={"uid":"PD1","name":"Pattern Descriptor 1","description":null,"entities":[{"uid":"L1","role":{"string":"Unlucky Lender"},"sameAs":null,"entities":null,"tags":null,"properties":null,"examplars":{"array":["l0376099"]},"weight":1.0},{"uid":"P1","role":{"string":"Risky Partner"},"sameAs":null,"entities":null,"tags":null,"properties":null,"examplars":{"array":["p137"]},"weight":1.0}],"links":[]}&max=20
        Object result = kivaBinding.searchByExample(query, "", 0, 4);
        AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);
        System.out.println("result = " + AvroUtils.encodeJSON((FL_PatternSearchResults) result));

        entityMatchDescriptors.clear();
        exemplars = Arrays.asList(new String[] { "l0376099" });
        entityMatchDescriptors.add(new FL_EntityMatchDescriptor("L1", "Unlucky Lender", null, null, null, exemplars, null));
        query.setEntities(entityMatchDescriptors);
        System.out.println("just lender query:");
        System.out.println(AvroUtils.encodeJSON(query));

        result = kivaBinding.searchByExample(query, "", 0, 4);
        System.out.println("result = " + result);

        entityMatchDescriptors.clear();
        exemplars = Arrays.asList(new String[] { "p137" });
        entityMatchDescriptors.add(new FL_EntityMatchDescriptor("P1", "Risky Partner", null, null, null, exemplars, null));
        query.setEntities(entityMatchDescriptors);
        System.out.println("just partner query:");
        System.out.println(AvroUtils.encodeJSON(query));
        result = kivaBinding.searchByExample(query, "", 0, 4);

        System.out.println("result = " + result);

        // FL_PatternDescriptor descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] {"lmarie8422",
        // "lmike1401", "p137"}));
        // FL_PatternDescriptor descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "lmarie8422",
        // "lmike1401", "lgooddogg1", "p137" }));
        FL_PatternDescriptor descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "lmarie8422",
                "lmike1401", "lgooddogg1", "ltrolltech4460", "p137", "p65" }));
        logger.debug("descriptor = " + AvroUtils.encodeJSON(descriptor));
        result = kivaBinding.searchByExample(descriptor, null, 0, 10);
        AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);
        // System.out.println("result = " + result);
        // System.out.println("result = " + AvroUtils.encodeJSON((FL_PatternSearchResults) result));
        
        if (true) {
            return;
        }

        logger.debug("--------------------------------");

        try {

            // loans for a lender
            // Graph loansForSkylar = kivaBinding.getOneHopGraph(LENDERS, "skylar", LOAN_META_DATA, 400);
            // logger.debug("got " + loansForSkylar.print());

            // teams for a lender
            // Graph teamForSkylar = kivaBinding.getOneHopGraph(LENDERS, "skylar", TEAMS, 400);
            // logger.debug("got " + teamForSkylar.print());

            // lenders for a team
            // Graph lendersForTeam = kivaBinding.getOneHopGraph(TEAMS, "2598", LENDERS, 400);
            // logger.debug("got " + lendersForTeam.print());

            // loans for a partner
            Graph loanForPartner = kivaBinding.getOneHopGraph(PARTNERS_META_DATA, "22", LOAN_META_DATA, 20);
            logger.debug("got " + loanForPartner.print() + "\npartner "
                    + loanForPartner.getNodes().iterator().next().props);
            // if (true) return;

            logger.debug("--------------------------------");

            // Map<String, String> entity = kivaBinding.getEntity("t" + "5048");
            // Collection<ResultInfo> entities = kivaBinding.getEntitiesForTag(FL_PropertyTag.ID, "5048", 20);
            // Collection<ResultInfo> entities = kivaBinding.getEntitiesForTag(FL_PropertyTag.LABEL, "skylar", 20);

//            FL_PropertyMatchDescriptor p1 = FL_PropertyMatchDescriptor.newBuilder()
//                    .setKey(FL_PropertyTag.LABEL.toString()).setConstraint(FL_Constraint.EQUALS).setValue("skylar")
//                    .build();
//            FL_PropertyMatchDescriptor p2 = FL_PropertyMatchDescriptor.newBuilder()
//                    .setKey(FL_PropertyTag.DATE.toString()).setConstraint(FL_Constraint.GREATER_THAN)
//                    .setValue("2012-04-01 01:01:01").build();
//
//            FL_PropertyMatchDescriptor p3 = FL_PropertyMatchDescriptor.newBuilder()
//                    .setKey(FL_PropertyTag.LABEL.toString()).setConstraint(FL_Constraint.FUZZY).setValue("Africa")
//                    .build();
//            Collection<ResultInfo> entities = kivaBinding.getEntitiesMatchingProperties(Arrays.asList(p3, p2), 10);
//            logger.debug("got " + entities.size() + " : ");
//            for (ResultInfo r : entities)
//                logger.debug("\t" + r);

            if (true)
                return;

            /*
             * kivaBinding.getEntity("l" +"skylar"); kivaBinding.getEntity("p" +"2"); kivaBinding.getEntity("t"
             * +"5048"); kivaBinding.getEntitiesByID(LENDERS, Arrays.asList("skylar", "METS"));
             */
            // ResultInfo whereabouts = kivaBinding.getEntitiesByID(LENDERS, "whereabouts", "San Francisco CA", 3);
            // Collection<ResultInfo> whereabouts = kivaBinding.getEntitiesByID("whereabouts", "San Francisco CA", 3);
            Collection<ResultInfo> whereabouts4 = kivaBinding.getEntities(
                    Arrays.asList(new Triple("whereabouts", "Francisco", "like")), 3);
            logger.debug("whereabouts " + whereabouts4);
            // if (true) return;
            Collection<ResultInfo> whereabouts3 = kivaBinding
                    .getEntities(Arrays.asList(new Triple("whereabouts", "San Francisco"), new Triple("category",
                            "Businesses")), 10);
            logger.debug("whereabouts " + whereabouts3);

            // if (true) return;
            /*
             * kivaBinding.getEntity(LOAN_META_DATA, "" + 84);
             */

            // kivaBinding.getBorrowersForLoan(""+511042);
//            FL_EntityMatchDescriptor lskylar = FL_EntityMatchDescriptor.newBuilder().setUid("123").setRole("")
//                    .setSameAs("").setEntities(Arrays.asList("lskylar")).build();
//
//            FL_PropertyMatchDescriptor whereabouts1 = FL_PropertyMatchDescriptor.newBuilder().setKey("whereabouts")
//                    .setConstraint(FL_Constraint.EQUALS).setValue("San Francisco").build();
//
//            FL_PropertyMatchDescriptor business = FL_PropertyMatchDescriptor.newBuilder().setKey("category")
//                    .setConstraint(FL_Constraint.EQUALS).setValue("Businesses").build();
//
//            FL_EntityMatchDescriptor whereaboutTest = FL_EntityMatchDescriptor.newBuilder().setUid("123").setRole("")
//                    .setSameAs("").setEntities(new ArrayList<String>())
//                    .setProperties(Arrays.asList(whereabouts1, business)).build();
//
//            System.out.println("descriptor : " + whereaboutTest);
//
//            FL_PatternDescriptor patternDescriptor = FL_PatternDescriptor.newBuilder().setUid("123").setName("")
//                    .setDescription("").setEntities(Arrays.asList(whereaboutTest))
//                    .setLinks(new ArrayList<FL_LinkMatchDescriptor>()).build();
//
//            FL_EntityMatchDescriptor entityMatchDescriptor = FL_EntityMatchDescriptor.newBuilder().setUid("123")
//                    .setRole("").setSameAs("").setEntities(new ArrayList<String>())
//                    .setProperties(Arrays.asList(p3, p2)).build();
//            FL_PatternDescriptor patternDescriptor2 = FL_PatternDescriptor.newBuilder().setUid("123").setName("")
//                    .setDescription("").setEntities(Arrays.asList(entityMatchDescriptor))
//                    .setLinks(new ArrayList<FL_LinkMatchDescriptor>()).build();
//
//            Object searchResult = kivaBinding.searchByExample(patternDescriptor2, "", 0, 20);
//            logger.debug("Result " + searchResult);
        } catch (Exception e) {
            e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
        }
    }
}
