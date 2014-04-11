package mitll.xdata.dataset.kiva.binding;

import influent.idl.*;
import mitll.xdata.NodeSimilaritySearch;
import mitll.xdata.binding.Binding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.hmm.VectorObservation;
import mitll.xdata.scoring.Transaction;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

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

  private NodeSimilaritySearch partnerIndex;
  private NodeSimilaritySearch lenderIndex;

  private PreparedStatement pairConnectedStatement;
  private PreparedStatement edgeMetadataKeyStatement;

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
    partnerIndex = new NodeSimilaritySearch(partnerFeatures);
    logger.debug("indexing lender features");
    lenderIndex = new NodeSimilaritySearch(lenderFeatures);
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
   * @see mitll.xdata.binding.BreadthFirstShortlist#getCandidateGraphs(java.util.List, int, boolean, String)
   * @param id
   * @param k
   * @param skipSelf
   * @return
   */
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

  /**
   * @see mitll.xdata.binding.BreadthFirstShortlist#getPatternSearchResults(java.util.List, java.util.List, java.util.Collection, mitll.xdata.binding.CandidateGraph)
   * @param id1
   * @param id2
   * @return
   */
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
   * @see mitll.xdata.binding.CartesianShortlist#connectedGroup(java.util.List)
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

  /**
   * @see mitll.xdata.binding.Shortlist#getLinks(java.util.List)
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

  /**
   * @see mitll.xdata.binding.Shortlist#getLinks(java.util.List)
   * @param id
   * @return
   */
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
  protected List<VectorObservation> createObservationVectors(List<Edge> edges, List<String> ids) {
    return null;
  }

  @Override
  protected List<FL_LinkMatchResult> createAggregateLinks(FL_PatternDescriptor example, FL_PatternSearchResult result, List<Edge> edges) {
    return null;
  }

}
