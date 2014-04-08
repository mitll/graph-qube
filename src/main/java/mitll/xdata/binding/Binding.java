package mitll.xdata.binding;

import influent.idl.FL_Constraint;
import influent.idl.FL_Entity;
import influent.idl.FL_EntityMatchDescriptor;
import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_EntityTag;
import influent.idl.FL_Link;
import influent.idl.FL_LinkMatchResult;
import influent.idl.FL_LinkTag;
import influent.idl.FL_PatternDescriptor;
import influent.idl.FL_PatternSearchResult;
import influent.idl.FL_PatternSearchResults;
import influent.idl.FL_Property;
import influent.idl.FL_PropertyMatchDescriptor;
import influent.idl.FL_PropertyTag;
import influent.idl.FL_PropertyType;
import influent.idl.FL_SearchResult;
import influent.idl.FL_SearchResults;
import influent.idl.FL_SingletonRange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import mitll.xdata.AvroUtils;
import mitll.xdata.PrioritizedCartesianProduct;
import mitll.xdata.dataset.kiva.binding.KivaBinding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.hmm.Hmm;
import mitll.xdata.hmm.KernelDensityLikelihood;
import mitll.xdata.hmm.ObservationLikelihood;
import mitll.xdata.hmm.StateSequence;
import mitll.xdata.hmm.VectorObservation;
import mitll.xdata.scoring.FeatureNormalizer;
import mitll.xdata.scoring.HmmScorer;
import mitll.xdata.scoring.Transaction;
import mitll.xdata.sql.SqlUtilities;

import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA. User: go22670 Date: 6/25/13 Time: 7:28 PM To change this template use File | Settings |
 * File Templates.
 */
public abstract class Binding extends SqlUtilities implements AVDLQuery {
  private static Logger logger = Logger.getLogger(Binding.class);

 // private static final boolean REVERSE_DIRECTION = false;
  private static final int DEFAULT_SHORT_LIST_SIZE = 100;
  public static final int MAX_CANDIDATES = 100;
  private static final long MB = 1024*1024;
  public static final int FULL_SEARCH_LIST_SIZE = 200;
	private static final int MAX_TRIES = 1000000;
	private static final double HMM_KDE_BANDWIDTH = 0.25;
	/**
	 * Scales distance between result probability and query probability when converting to score. Lower makes scores
	 * look higher.
	 */
	private static final double HMM_SCALE_DISTANCE = 0.1;
	private static final boolean SKIP_SELF_AS_NEIGHBOR = false;

	protected Connection connection;
	protected Map<String, Collection<String>> tableToColumns = new HashMap<String, Collection<String>>();
	protected Map<String, Collection<String>> columnToTables = new HashMap<String, Collection<String>>();
	protected Map<String, String> tableToPrimaryKey = new HashMap<String, String>();

	private static final boolean LIMIT = false;
	protected Map<FL_PropertyTag, List<String>> tagToColumn = new HashMap<FL_PropertyTag, List<String>>();
  protected Map<String, String> prefixToTable = new HashMap<String, String>();
  protected Map<String, Map<String, KivaBinding.ForeignLink>> sourceToTargetToLinkTable = new HashMap<String, Map<String, KivaBinding.ForeignLink>>();
	// Map<String, ForeignLink> tableToLinkTable = new HashMap<String, ForeignLink>();
  protected Map<String, String> tableToDisplay = new HashMap<String, String>();
	private boolean showSQL = false;
	private boolean showResults = false;
  protected final Map<String, Set<String>> stot = new HashMap<String, Set<String>>();
  protected Set<String> validTargets = new HashSet<String>();

  public Binding(DBConnection connection) {
		try {
			this.connection = connection.getConnection();
		} catch (Exception e) {
			logger.error("got " + e, e);
		}
	}

	protected Binding() {
	}

	protected void populateColumnToTables() {
		for (Map.Entry<String, Collection<String>> kv : tableToColumns.entrySet()) {
			for (String col : kv.getValue()) {
				Collection<String> tables = columnToTables.get(col);
				if (tables == null)
					columnToTables.put(col, tables = new ArrayList<String>());
				tables.add(kv.getKey());
			}
		}
	}

	/**
	 * @see #getMatchingRows(String)
	 * @param rs
	 * @param nameToType
	 * @return
	 * @throws SQLException
	 */
	private Map<String, String> getRow(ResultSet rs, Map<String, String> nameToType) throws SQLException {
		Map<String, String> row = new TreeMap<String, String>();
		for (Map.Entry<String, String> kv : nameToType.entrySet()) {
			String type = kv.getValue();
			String name = kv.getKey();
			// logger.debug(name +" -> " + type);
			if (type.equals("VARCHAR")) {
				row.put(name, rs.getString(name));
			} else if (type.equals("INT") || type.equals("TINYINT") || type.equals("INTEGER")) {
				row.put(name, "" + rs.getInt(name));
			} else if (type.equals("BIGINT")) {
				row.put(name, "" + rs.getLong(name));
			} else if (type.equals("DATETIME") || type.equals("TIMESTAMP")) {
				try {
					row.put(name, "" + rs.getTimestamp(name));
				} catch (SQLException e) {
					row.put(name, "");
				}
			} else if (type.equals("DECIMAL")) {
				row.put(name, "" + rs.getFloat(name));
			} else if (type.equals("CHAR")) {
				row.put(name, rs.getString(name));
			} else if (type.equals("DOUBLE")) {
				row.put(name, "" + rs.getDouble(name));
			} else {
				logger.warn("Got unhandled type " + type + " for " + name);
			}
		}
		return row;
	}

  /**
   * @see #getEntities(String, java.util.List)
   * @param table
   * @param constraint
   * @param limit
   * @return
   * @throws Exception
   */
	private ResultInfo getEntitiesWhere(String table, String constraint, long limit) throws Exception {
		String sql = "SELECT * FROM " + table + " where " +

		constraint +

		(LIMIT ? " limit 0, 10" : " limit 0," + limit) + ";";

		ResultInfo matchingRows = getMatchingRows(sql);
		matchingRows.setTable(table);
		return matchingRows;
	}

	private ResultInfo getMatchingRows(String sql) throws Exception {
		if (showSQL) {
			logger.debug("getMatchingRows : doing " + sql);
    }
		PreparedStatement statement = connection.prepareStatement(sql);
		ResultSet rs = null;
		try {
			rs = statement.executeQuery();
		} catch (Exception e) {
			logger.error("got " + e + " doing " + sql, e);
			throw e;
		}
		Map<String, String> nameToType = getNameToType(rs);

		List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
		while (rs.next()) {
			Map<String, String> row = getRow(rs, nameToType);
			rows.add(row);
		}
		if (showResults) {
			logger.debug("Got " + rows.size() + " ");
			for (Map<String, String> row : rows)
				logger.debug(row);
		}
    //logger.debug("getMatchingRows : Got " + rows.size() + " ");

    rs.close();
		statement.close();
		return new ResultInfo(nameToType, rows);
	}

	/**
	 * Get entities by id
	 *
	 * @see #getEntitiesByID(influent.idl.FL_EntityMatchDescriptor)
	 * @param table
	 * @param ids
	 * @return
	 */
	protected ResultInfo getEntities(String table, List<String> ids) {
		try {
			StringBuilder builder = new StringBuilder();
			for (String id : ids)
				builder.append("'" + id + "', ");
			String list = builder.toString();
			// logger.debug("list " + list);
			String column = tableToPrimaryKey.get(table);
			String constraint = column + " in (" + list.substring(0, list.length() - 2) + ") ";

      ResultInfo entitiesWhere = getEntitiesWhere(table, constraint, ids.size());
      if (entitiesWhere.rows.isEmpty()) {
        logger.error("huh? can't find " + ids + " entities in " +table + " under col " + column);
      }
      return entitiesWhere;
		} catch (Exception ee) {
			logger.error("looking for " + ids + " got error " + ee, ee);
		}
		return new ResultInfo();
	}

	/**
	 * @see KivaBinding#getSearchResult(influent.idl.FL_EntityMatchDescriptor, java.util.List, long)
	 * @param properties
	 * @param limit
	 * @return
	 */
	protected Collection<ResultInfo> getEntitiesMatchingProperties(List<FL_PropertyMatchDescriptor> properties,
			long limit) {
		List<Triple> triples = getTriples(properties);
		return getEntities(triples, limit);
	}

	private List<Triple> getTriples(List<FL_PropertyMatchDescriptor> props) {
		List<Triple> triples = new ArrayList<Triple>();
		for (FL_PropertyMatchDescriptor prop : props) {
			boolean isTag = prop.getKey().toUpperCase().equals(prop.getKey())
					&& FL_PropertyTag.valueOf(prop.getKey()) != null;
			if (isTag) {
				FL_PropertyTag fl_propertyTag = FL_PropertyTag.valueOf(prop.getKey());
				if (fl_propertyTag != null) {
					Triple t = new Triple(prop);
					for (String col : getColumnsForTag(fl_propertyTag)) {
						triples.add(new Triple(col, t.value, t.operator));
					}
				}
			} else {
				triples.add(new Triple(prop));
			}
		}
		return triples;
	}

	/**
	 * Get entities where prop=value
	 *
	 * @param key
	 * @param value
	 * @param limit
	 * @return
	 */
	private Collection<ResultInfo> getEntities(String key, String value, long limit) {
		return getEntities(Arrays.asList(new Triple(key, value, "=")), limit);
	}

	/**
	 * Get entities matching triple, where triple column could be a column in multiple tables For instance Kiva has
	 * multiple tables with column "whereabouts" so you get back entities of multiple types that each have a whereabouts
	 * column.
	 *
	 * @see #getEntities(String, String, long)
	 * @see #getEntitiesMatchingProperties(java.util.List, long)
	 * @param triples
	 * @param limit
	 * @return
	 */
	protected Collection<ResultInfo> getEntities(List<Triple> triples, long limit) {
		List<ResultInfo> results = new ArrayList<ResultInfo>();

		Map<String, List<Triple>> tableToTriples = new HashMap<String, List<Triple>>();
		// logger.debug("getEntitiesByID for " + triples);
		for (Triple t : triples) {
			Collection<String> tables = columnToTables.get(t.key);
			if (tables == null)
				logger.warn("huh? no table for " + t.key + " in " + columnToTables.keySet());
			else {
				for (String table : tables) {
					List<Triple> triplesForTable = tableToTriples.get(table);
					if (triplesForTable == null)
						tableToTriples.put(table, triplesForTable = new ArrayList<Triple>());
					triplesForTable.add(t);
				}
			}
		}

		// logger.debug("getEntitiesByID examining " + tableToTriples.keySet() + " tables ");

		for (Map.Entry<String, List<Triple>> tableToSearchCriteria : tableToTriples.entrySet()) {
			ResultInfo entities = getEntities(tableToSearchCriteria.getKey(), Collections.EMPTY_LIST,
					tableToSearchCriteria.getValue(), limit);
			if (!entities.isEmpty())
				results.add(entities);

		}
		logger.debug("getEntitiesByID for " + triples + " returned " + results);

		return results;
	}

	/**
	 * Map property tag to column
	 *
	 * @param tag
	 * @return
	 */
	private List<String> getColumnsForTag(FL_PropertyTag tag) {
		if (tagToColumn.containsKey(tag))
			return tagToColumn.get(tag);
		else
			return Collections.emptyList();
	}

	protected void addTagToColumn(FL_PropertyTag tag, String col) {
		List<String> cols = tagToColumn.get(tag);
		if (cols == null)
			tagToColumn.put(tag, cols = new ArrayList<String>());
		cols.add(col);
	}

	protected void populateTableToColumns(DBConnection connection, Collection<String> tablesToQuery, String dbType) {
		for (String table : tablesToQuery) {
			try {
				Collection<String> columns = getColumns(table, connection.getConnection(), dbType);
				tableToColumns.put(table, columns);
			} catch (SQLException e) {
				logger.error("looking at " + table + " got " + e, e);
			}
		}
	}

	/**
	 * Just for testing... assumes id has a one character prefix indicating what flavor of data it is, e.g.
	 * <p/>
	 * lenders - 'l'+lenders_lenderId partners - 'p'+partners_id borrowers/loans - 'b'+loans_id brokers
	 * 'p'+partners_id+'-'+loans_id
	 *
	 * @param id
	 * @return first of all possible matches...
	 */
	public Map<String, String> getEntity(String id) {
		try {
			String table = getTableForID(id);
			if (table == null)
				return null;

			ResultInfo entities = getEntities(table, Arrays.asList(id.substring(1)));
			if (entities.rows.isEmpty())
				return null;
			else
				return entities.rows.get(0);
		} catch (Exception ee) {
			logger.error("looking for " + id + " got error " + ee, ee);
		}
		return null;
	}

	public ResultInfo getEntityResult(String id) {
		logger.debug("ENTER getEntityResult: id = " + id);
		try {
			String table = getTableForID(id);
			logger.debug("table = " + table);
			if (table == null)
				return null;

			ResultInfo entities = getEntities(table, Arrays.asList(id.substring(1)));
			if (entities.rows.isEmpty())
				return null;
			else
				return entities;
		} catch (Exception ee) {
			logger.error("looking for " + id + " got error " + ee, ee);
		}
		return null;
	}

	public List<Binding.ResultInfo> getEntities(FL_PatternDescriptor example) {
		List<String> exemplarIDs = getExemplarIDs(example);
		List<Binding.ResultInfo> entities = new ArrayList<Binding.ResultInfo>();
		for (String id : exemplarIDs) {
			Binding.ResultInfo entity = getEntityResult(id);
			entities.add(entity);
		}
		return entities;
	}

	/**
	 * Map id prefix to table
	 *
	 * @param id
	 * @return
	 */
	protected String getTableForID(String id) {
		String table = "";
		for (String pre : prefixToTable.keySet()) {
			if (id.startsWith(pre))
				table = prefixToTable.get(pre);
		}
		if (table.length() == 0)
			return null;
		return table;
	}

	/*
	 * private Map<String, String> getEntity(String table, String id) { ResultInfo entities = getEntitiesByID(table
	 * ,Arrays.asList(id)); if (entities.rows.isEmpty()) return null; else return entities.rows.get(0); }
	 */

	/**
	 * Get entities by getting ids from entity match descriptor (ignoring the other fields)
	 *
	 * @see KivaBinding#searchByExample(influent.idl.FL_PatternDescriptor, String, long, long)
	 * @param descriptor
	 * @return
	 */
	protected ResultInfo getEntitiesByID(FL_EntityMatchDescriptor descriptor) {
		List<String> entityIDs = descriptor.getEntities();
		if (entityIDs != null && !entityIDs.isEmpty()) {
			String id = entityIDs.iterator().next();
			String table = getTableForID(id);
			if (table == null) {
				// return a dummy entity with just that id
				return createDummyEntity(id);
			}
			List<String> trim = new ArrayList<String>();
			for (String eid : entityIDs)
				trim.add(eid.substring(1));

			return getEntities(table, trim);
		} else {
			logger.warn("no entities on descriptor " + descriptor.getUid());
		}
		return new ResultInfo();
	}

	protected ResultInfo createDummyEntity(String id) {
		Map<String, String> nameToType = new HashMap<String, String>();
		nameToType.put("node_id", "BIGINT");
		List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
		Map<String, String> row = new HashMap<String, String>();
		row.put("node_id", id);
		rows.add(row);
		ResultInfo result = new ResultInfo(nameToType, rows);
		return result;
	}

	/**
	 * Get entities by property tag and value
	 *
	 * So this lets you talk about columns in abstract terms LABEL=Bob (instead of needing to know the name of the
	 * column)
	 *
	 * @param tag
	 * @param value
	 * @param limit
	 * @return
	 */
	private Collection<ResultInfo> getEntitiesForTag(FL_PropertyTag tag, String value, long limit) {
		List<String> columnsForTag = getColumnsForTag(tag);
		List<ResultInfo> results = new ArrayList<ResultInfo>();
		if (columnsForTag.isEmpty())
			return Collections.emptyList();
		else {
			for (String col : columnsForTag) {
				Collection<ResultInfo> entities = getEntities(col, value, limit);
				// logger.debug("getEntitiesForTag for " + col + "=" + value + " got " + entities);
				results.addAll(entities);
			}
		}
		/*
		 * logger.debug("got " + results.size() + " results for " + tag + "=" + value);
		 */

		return results;
	}

	/**
	 * Get entity as name=value pairs, for a property tag and value
	 *
	 * @param tag
	 * @param value
	 * @return
	 */
	protected Map<String, String> getEntity(FL_PropertyTag tag, String value) {
		Collection<ResultInfo> entitiesForTag = getEntitiesForTag(tag, value, 1);
		if (entitiesForTag.isEmpty()) {
			logger.debug("no results for " + tag + "=" + value);
			return Collections.emptyMap();
		} else {
			List<Map<String, String>> rows = entitiesForTag.iterator().next().rows;
			if (rows.isEmpty())
				return Collections.emptyMap();
			else
				return rows.iterator().next();
		}
	}

	/**
	 * Assume for the moment that you can either ask for an entity by id or by some set of properties
	 *
	 * @see mitll.xdata.dataset.kiva.binding.KivaBinding#searchByExample(influent.idl.FL_PatternDescriptor, String, long, long)
	 * @param properties
	 * @param max
	 * @return
	 */
	@Override
	public FL_SearchResults getSimpleSearchResult(List<FL_PropertyMatchDescriptor> properties, long max) {
		FL_SearchResults patternSearchResult = new FL_SearchResults();

		Collection<ResultInfo> entitiesMatchingProperties = getEntitiesMatchingProperties(properties, max);
		for (ResultInfo entity : entitiesMatchingProperties) {

			for (Map<String, String> entityMap : entity.rows) {
				FL_SearchResult entityMatchResult = makeEntitySearchResult(entity, entityMap);
				patternSearchResult.getResults().add(entityMatchResult);
			}
		}

		patternSearchResult.setTotal(new Long(entitiesMatchingProperties.size()));

		return patternSearchResult;
	}

	/**
	 * Assume for the moment that you can either ask for an entity by id or by some set of properties
	 *
	 * @seex mitll.xdata.dataset.kiva.binding.KivaBinding#searchByExample_ORIG(influent.idl.FL_PatternDescriptor, String, long, long)
	 * @param descriptor
	 * @param properties
	 * @param max
	 * @return
	 */
	@Override
	public FL_PatternSearchResults getSearchResult(FL_EntityMatchDescriptor descriptor,
			List<FL_PropertyMatchDescriptor> properties, long max) {
		List<FL_PatternSearchResult> results = new ArrayList<FL_PatternSearchResult>();
		FL_PatternSearchResult patternSearchResult = new FL_PatternSearchResult();
		patternSearchResult.setScore(1.0);
		patternSearchResult.setLinks(new ArrayList<FL_LinkMatchResult>());
		patternSearchResult.setEntities(new ArrayList<FL_EntityMatchResult>());

		if (properties.isEmpty()) {
			ResultInfo entities = getEntitiesByID(descriptor);

			for (Map<String, String> entityMap : entities.rows) {
				FL_EntityMatchResult entityMatchResult = makeEntityMatchResult(descriptor, entities, entityMap);
				patternSearchResult.getEntities().add(entityMatchResult);

				// add to list of results
			}
		} else {
			Collection<ResultInfo> entitiesMatchingProperties = getEntitiesMatchingProperties(properties, max);
			for (ResultInfo entity : entitiesMatchingProperties) {

				for (Map<String, String> entityMap : entity.rows) {
					FL_EntityMatchResult entityMatchResult = makeEntityMatchResult(descriptor, entity,
					// primaryKeyCol,
							entityMap);
					patternSearchResult.getEntities().add(entityMatchResult);
				}
			}
		}

		results.add(patternSearchResult);

		FL_PatternSearchResults queryResult = new FL_PatternSearchResults();
		queryResult.setResults(results);
		queryResult.setTotal((long) results.size());
		return queryResult;

	}

	/**
	 * Convert one ResultInfo item map into one EntityMatchResult
	 *
	 * @see #getSearchResult(influent.idl.FL_EntityMatchDescriptor, java.util.List, long)
	 * @param descriptor
	 * @param entities
	 * @param entityMap
	 * @return equivalent to entityMap
	 */
	protected FL_EntityMatchResult makeEntityMatchResult(FL_EntityMatchDescriptor descriptor, ResultInfo entities,
			Map<String, String> entityMap) {

		String primaryKeyCol = tableToPrimaryKey.get(entities.getTable());

		FL_Entity entity = makeEntity(entities.nameToType, primaryKeyCol, entityMap);
		entity.getProperties().add(createTypeProperty(tableToDisplay.get(entities.getTable())));
		// wrap entity in FL_EntityMatchResult
		FL_EntityMatchResult entityMatchResult = new FL_EntityMatchResult();
		entityMatchResult.setScore(1.0);
		entityMatchResult.setUid(descriptor.getUid());
		entityMatchResult.setRole(descriptor.getRole());
		entityMatchResult.setEntity(entity);
		return entityMatchResult;
	}

	/**
	 * @see #getSimpleSearchResult(java.util.List, long)
	 * @param entities
	 * @param entityMap
	 * @return
	 */
	private FL_SearchResult makeEntitySearchResult(ResultInfo entities, Map<String, String> entityMap) {
		String primaryKeyCol = tableToPrimaryKey.get(entities.getTable());

		FL_Entity entity = makeEntity(entities.nameToType, primaryKeyCol, entityMap);
		entity.getProperties().add(createTypeProperty(tableToDisplay.get(entities.getTable())));
		// wrap entity in FL_EntityMatchResult
		FL_SearchResult entityMatchResult = new FL_SearchResult();
		entityMatchResult.setScore(1.0);
		entityMatchResult.setResult(entity);
		return entityMatchResult;
	}

	/**
	 * Convert the results (ResultInfo, really) into a avdl Entity.
	 *
	 * @see #makeEntityMatchResult(influent.idl.FL_EntityMatchDescriptor, mitll.xdata.binding.Binding.ResultInfo,
	 *      java.util.Map)
	 * @param colToType
	 * @param primaryKeyCol
	 * @param entityMap
	 * @return
	 */
	protected FL_Entity makeEntity(Map<String, String> colToType, String primaryKeyCol, Map<String, String> entityMap) {
		FL_Entity entity = new FL_Entity();
		entity.setTags(new ArrayList<FL_EntityTag>()); // none for now...

		List<FL_Property> properties = new ArrayList<FL_Property>();
		entity.setProperties(properties);

		String uid = setProperties(properties, colToType, primaryKeyCol, entityMap);

		entity.setUid(uid);
		return entity;
	}

	/**
	 * Make AVDL properties with AVDL types (mapped from SQL types)
	 *
	 * @param properties
	 * @param colToType
	 * @param primaryKeyCol
	 * @param entityMap
	 * @return entity id from entity map
	 */
	protected String setProperties(List<FL_Property> properties, Map<String, String> colToType, String primaryKeyCol,
			Map<String, String> entityMap) {
		String entityID = null;
		for (Map.Entry<String, String> kv : entityMap.entrySet()) {
			if (kv.getKey().equals(primaryKeyCol)) {
				entityID = kv.getValue();
				// entity.setUid(entityID);

			} else {
				String typeForColumn = colToType.get(kv.getKey());
				FL_PropertyType type = FL_PropertyType.STRING;
				if (typeForColumn.equals("VARCHAR")) {
					type = FL_PropertyType.STRING;
				} else if (typeForColumn.equals("CHAR")) {
					type = FL_PropertyType.STRING;
				} else if (typeForColumn.equals("INT")) {
					type = FL_PropertyType.LONG;
				} else if (typeForColumn.equals("INTEGER")) {
					type = FL_PropertyType.LONG;
				} else if (typeForColumn.equals("BIGINT")) {
					type = FL_PropertyType.LONG;
				} else if (typeForColumn.equals("DECIMAL")) {
					type = FL_PropertyType.DOUBLE;
				} else if (typeForColumn.equals("DOUBLE")) {
					type = FL_PropertyType.DOUBLE;
				} else if (typeForColumn.equals("DATETIME")) {
					type = FL_PropertyType.DATE;
				} else if (typeForColumn.equals("TIMESTAMP")) {
					type = FL_PropertyType.DATE;
				} else {
					logger.debug("unknown type: " + typeForColumn);
				}

				// logger.debug("kv.getKey() = " + kv.getKey() + ", kv.getValue() = " + kv.getValue() + ", type = " +
				// type);
				if (kv.getValue() != null) {
					properties.add(createProperty(kv.getKey(), kv.getValue(), type));
				} else {
					properties.add(createProperty(kv.getKey(), "null", type));
				}
			}
		}
		return entityID;
	}

	private FL_Property createTypeProperty(String type) {
		FL_Property property = new FL_Property();
		property.setKey(FL_PropertyTag.TYPE.toString());
		property.setFriendlyText(type == null ? "null" : type);
		property.setRange(new FL_SingletonRange(type == null ? "null" : type, FL_PropertyType.STRING));
		property.setTags(Arrays.asList(FL_PropertyTag.TYPE));
		return property;
	}

	protected FL_Property createProperty(String key, Object value, FL_PropertyType type) {
		// TODO: add support for FL_PropertyTag
		FL_Property property = new FL_Property();
		property.setKey(key);
		property.setFriendlyText(value.toString());
		property.setRange(new FL_SingletonRange(value, type));
		property.setTags(new ArrayList<FL_PropertyTag>());
		return property;
	}

	public FL_SearchResults simpleSearch(List<FL_PropertyMatchDescriptor> properties, long start, long max) {
		// logger.debug("max " + max);
		if (max == 0) {
			logger.warn("max given as 0, using 10 instead...");
			max = 10;
		}
		if (start > max)
			start = max - 1;

		// logger.debug("searchByExample : got " +properties);
		return getSimpleSearchResult(properties, max);

	}

	public String toString() {
		return prefixToTable.toString();
	}

	public static class Triple {
		String key, value, operator;

		public Triple(FL_PropertyMatchDescriptor prop) {
			// TODO : handle non-singleton ranges?
			this(prop.getKey(), ((FL_SingletonRange) prop.getRange()).getValue().toString(), "");
			this.operator = getOperatorForConstraint(prop.getConstraint());
		}

		public Triple(String key, String value) {
			this(key, value, "=");
		}

		public Triple(String key, String value, String operator) {
			this.key = key;
			this.value = value;
			this.operator = operator;
		}

		public String toSQL() {
			return (operator.equals("like")) ? key + " " + operator + " '%" + value + "%'" : key + " " + operator
					+ " '" + value + "'";
		}

		private String getOperatorForConstraint(FL_Constraint propertyConstraint) {
			String operator = "";
			if (FL_Constraint.REQUIRED_EQUALS == propertyConstraint) {
				operator = "=";
			} else if (FL_Constraint.NOT == propertyConstraint) {
				operator = "<>";
			} else if (FL_Constraint.FUZZY_PARTIAL_OPTIONAL == propertyConstraint) {
				operator = "like";
			}
			return operator;
		}

		public String toString() {
			return key + " " + operator + " " + value;
		}
	}

	/**
	 * Get entities matching one constraint
	 *
	 * @param table
	 * @param key
	 * @param value
	 * @param limit
	 * @return
	 */
	protected ResultInfo getEntities(String table, String key, String value, long limit) {
		return getEntities(table, Arrays.asList(new Triple(key, value, "=")), limit);
	}

	/**
	 * Get entities matching all constraints
	 *
	 * @param table
	 * @param triples
	 * @param limit
	 * @return
	 */
	protected ResultInfo getEntities(String table, Collection<Triple> triples, long limit) {
		return getEntities(table, Collections.EMPTY_LIST, triples, limit);
	}

	/**
	 * Get set of entities matching any of the constraints
	 *
	 * @param table
	 * @param triples
	 * @param limit
	 * @return
	 */
	private ResultInfo getOrEntities(String table, Collection<Triple> triples, long limit) {
		return getEntities(table, triples, Collections.EMPTY_LIST, limit);
	}

	/**
	 * Get a collection of entities (e.g. a set of ids) under the constaints of the andTriples
	 *
	 * @see #getEntities(java.util.List, long) TODO : use a PrepareStatement with '?' mapped to arguments...
	 * @param table
	 * @param limit
	 * @return
	 */
  private ResultInfo getEntities(String table, Collection<Triple> orTriples, Collection<Triple> andTriples,
			long limit) {
		try {
			String constraint = "";

			String booleanOp = " OR ";
			for (Triple t : orTriples) {
				constraint += t.toSQL() + booleanOp;
			}
			if (!orTriples.isEmpty())
				constraint = "(" + constraint.substring(0, constraint.length() - booleanOp.length()) + ")";

			String constraint2 = "";

			booleanOp = " AND ";
			for (Triple t : andTriples) {
				constraint2 += t.toSQL() + booleanOp;
			}
			if (!andTriples.isEmpty())
				constraint2 = "(" + constraint2.substring(0, constraint2.length() - booleanOp.length()) + ")";

			if (constraint.isEmpty())
				constraint = constraint2;
			else if (!constraint2.isEmpty())
				constraint += " AND " + constraint2;

			return getEntitiesWhere(table, constraint, limit);
		} catch (Exception ee) {
			logger.error("looking for " + orTriples + " and " + andTriples + " got error " + ee, ee);
		}
		return new ResultInfo();
	}

	/**
	 * @return k nearest neighbors to node
   * @see #getShortlist(java.util.List, java.util.List, long)
	 */
	protected abstract List<String> getNearestNeighbors(String id, int k, boolean skipSelf);

	/**
	 * @return similarity between two nodes
	 */
	protected abstract double getSimilarity(String id1, String id2);

	public static List<String> getExemplarIDs(FL_PatternDescriptor patternDescriptor) {
		List<String> ids = new ArrayList<String>();
		if (patternDescriptor == null)
			return ids;
		List<FL_EntityMatchDescriptor> descriptors = patternDescriptor.getEntities();
		if (descriptors == null)
			return ids;

		for (FL_EntityMatchDescriptor descriptor : descriptors) {
			ids.add(descriptor.getExamplars().get(0));
			if (descriptor.getExamplars().size() > 1) {
				logger.debug("more than 1 exemplar id in FL_EntityMatchDescriptor: " + descriptor.getExamplars());
			}
		}
		return ids;
	}

	public static List<String> getEntityIDs(List<FL_EntityMatchResult> entities) {
		List<String> ids = new ArrayList<String>();
		for (FL_EntityMatchResult entity : entities) {
			ids.add(entity.getEntity().getUid());
		}
		return ids;
	}

	/**
	 * @see mitll.xdata.SimplePatternSearch#searchByExample
	 * @param example
	 * @param ignoredService
	 * @param start
	 * @param max
	 * @return
	 */
	public Object searchByExample(FL_PatternDescriptor example, String ignoredService, long start, long max) {
		// original service
		return searchByExample(example, ignoredService, start, max, false, Long.MIN_VALUE, Long.MAX_VALUE);
	}

  public static final int MAX_TEXT_LENGTH = 15;

  /**
   * Return json with nodes and links, suitable for a sankey display
   *
   * @param ids
   * @param start
   * @param max
   * @param startTime
   * @param endTime
   * @return
   */
  public String searchByExampleJson(List<String> ids,long start, long max, long startTime, long endTime) {
    FL_PatternSearchResults fl_patternSearchResults = searchByExample(ids, start, FULL_SEARCH_LIST_SIZE, startTime, endTime);
     logger.debug("searchByExampleJson : got " + fl_patternSearchResults.getResults().size() + " results");
     Set<String> querySet = new HashSet<String>(ids);

    FL_PatternSearchResult queryResult = null;
    for (FL_PatternSearchResult result : fl_patternSearchResults.getResults()) {

      Set<String> resultSet = new HashSet<String>();

      for (FL_EntityMatchResult entity : result.getEntities()) {
         FL_Entity entity1 = entity.getEntity();
          boolean found = false;
         for (FL_Property prop : entity1.getProperties()) {
           // logger.debug("got " + prop.getKey() + " : " + prop.getValue());
           if (prop.getKey().equalsIgnoreCase("name") || prop.getKey().equalsIgnoreCase("node_id")) {
             // keyValue.put(prop.getKey(),prop.getFriendlyText());
             String friendlyText = prop.getFriendlyText();
            // if (friendlyText.length() > MAX_TEXT_LENGTH) friendlyText = friendlyText.substring(0, MAX_TEXT_LENGTH) + "...";
             resultSet.add(friendlyText);
             found =true;
           }
           if (!found) resultSet.add(entity1.getUid());
           //if (prop.getKey().equals("uid")) uidValue = ((FL_SingletonRange) prop.getRange()).getValue().toString();
         }
       }
      if (resultSet.size() == querySet.size() && resultSet.equals(querySet)) {
        logger.debug("\n\n\n\n found query set ");
        queryResult = result;
      }
      else {
        //logger.debug("looking for " + querySet + " but not this one "+ resultSet);
      }
    }

    if (queryResult == null) {
      logger.error("\n\n\ncouldn't find the query result!\n\n\n");
    }

    String json = "{\"query\" :\n"+getJsonForResult(queryResult);
                       json += ",\n\"results\":[\n";
    int count = 0;
    for (FL_PatternSearchResult result : fl_patternSearchResults.getResults()) {
      if (result != queryResult) {
      json += getJsonForResult(result) +",\n";
        count++;
      }
      if (count == max) break;
    }
    json = json.substring(0,json.length()-2);

    json += "]}";
    return json;
  }

  private String getJsonForResult(FL_PatternSearchResult result) {
    StringBuilder builder = new StringBuilder();

    builder.append("{\"nodes\":[\n");
    List<String> names = new ArrayList<String>();
    if (result != null) {
      List<FL_EntityMatchResult> entities = result.getEntities();
      for (FL_EntityMatchResult entity : entities) {
        FL_Entity entity1 = entity.getEntity();
        // String uidValue = entity1.getUid();
        boolean found = false;
        for (FL_Property prop : entity1.getProperties()) {
          // logger.debug("got " + prop.getKey() + " : " + prop.getValue());
          if (prop.getKey().equalsIgnoreCase("name") || prop.getKey().equalsIgnoreCase("node_id")) {
            // keyValue.put(prop.getKey(),prop.getFriendlyText());
            String friendlyText = prop.getFriendlyText();
            if (friendlyText.length() > MAX_TEXT_LENGTH)
              friendlyText = friendlyText.substring(0, MAX_TEXT_LENGTH) + "...";
            builder.append("    {\"name\":\"" + friendlyText + "\"},\n");
            names.add(friendlyText);
            found = true;
          }
          //if (prop.getKey().equals("uid")) uidValue = ((FL_SingletonRange) prop.getRange()).getValue().toString();
        }
        if (!found) {
          builder.append("    {\"name\":\"" + entity1.getUid() + "\"},\n");
          names.add(entity1.getUid());
        }

      }
      trimLast(builder);
    }


    builder.append("],\n\"links\":[\n");

    // dump overall links
    PatternSearchResultWithState searchResultWithState = (PatternSearchResultWithState) result;
    List<FL_LinkMatchResult> objects = Collections.emptyList();
    List<FL_LinkMatchResult> links = searchResultWithState == null ? objects : searchResultWithState.getLinks();
    Map<String, Integer> objectObjectMap = Collections.emptyMap();
    Map<String, Integer> pairToID = dumpLinks(builder, names, links, objectObjectMap);

    if (searchResultWithState != null) {
      for (List<FL_LinkMatchResult> linksForPhase : searchResultWithState.getPhaseLinks()) {
        dumpLinks(builder, names, linksForPhase, pairToID);
      }
      builder.replace(builder.length() - 1, builder.length(), "\n");
    }

    builder.append("]}\n");

    return builder.toString();
  }

  private void trimLast(StringBuilder builder) {
    builder.replace(builder.length()-2,builder.length(),"\n");
  }

  private Map<String,Integer> dumpLinks(StringBuilder builder, List<String> names, List<FL_LinkMatchResult> links,
                                        Map<String,Integer> inPairToID) {
    int id = 0;
    Map<String,Integer> pairToID = new HashMap<String, Integer>();
    inPairToID = new HashMap<String, Integer>(inPairToID);
    builder.append(" [\n");  // TODO !!!! just for testing -- need one set of links for each phase
    for (FL_LinkMatchResult link : links) {
      FL_Link link1 = link.getLink();
      String source = link1.getSource();
      int sindex = names.indexOf(source);
      String target = link1.getTarget();
      int tindex = names.indexOf(target);

      String value = "";
      for (FL_Property property : link1.getProperties()) {
        if (property.getKey().equals("netFlow")) value = property.getFriendlyText();
      }

      boolean b = value.startsWith("-");// && REVERSE_DIRECTION;
      String key = source + "-" + target;
   //   if (b) logger.info("reverse direction of " +key);
      int idToUse = id++;
      if (!inPairToID.isEmpty()) {
        if (!inPairToID.containsKey(key)) logger.error("huh? can't find source-target pair " +key);
        else {
          idToUse = inPairToID.remove(key);
        }
      }
      else {
        pairToID.put(key,idToUse);
      }
      writeOneLink(builder, sindex, tindex, value, b, idToUse);
    }
    for (Map.Entry<String, Integer> zeroLinks : inPairToID.entrySet()) {
      String[] split = zeroLinks.getKey().split("-");

      int sindex = names.indexOf(split[0]);
      int tindex = names.indexOf(split[1]);
      writeOneLink(builder, sindex, tindex, "0", false, zeroLinks.getValue());

    }
    trimLast(builder);
    builder.append("\n],");  // TODO !!!! just for testing -- need one set of links for each phase
    return pairToID;
  }

  private void writeOneLink(StringBuilder builder, int sindex, int tindex, String value, boolean b, int idToUse) {
    builder.append("  {\"id\":" +
        idToUse +
        ",\"source\":" +
        (b ? tindex : sindex) +
        ",\"target\":" +
        (b ? sindex : tindex) +
        ",\"value\":" +
        (b ? value.substring(1) : value) +
        "},\n");
  }

  /**
   * @see #searchByExampleJson(java.util.List, long, long, long, long)
   * @param ids
   * @param start
   * @param max
   * @param startTime
   * @param endTime
   * @return
   */
  private FL_PatternSearchResults searchByExample(List<String> ids,long start, long max, long startTime, long endTime) {
    FL_PatternDescriptor descriptor = AvroUtils.createExemplarQuery(ids);
    return searchByExample(descriptor,"",start,max, true,startTime,endTime);
  }

	/**
	 * @see mitll.xdata.SimplePatternSearch#searchByExample(influent.idl.FL_PatternDescriptor, String, long, long, boolean)
   * @see #searchByExample(java.util.List, long, long, long, long)
	 * @param example
	 * @param ignoredService
	 * @param start ignored for now
	 * @param max items to return
	 * @param rescoreWithHMM
	 * @return
	 */
	public FL_PatternSearchResults searchByExample(FL_PatternDescriptor example, String ignoredService, long start, long max,
                                                 boolean rescoreWithHMM, long startTime, long endTime) {
		long then = System.currentTimeMillis();
		logger.debug("ENTER searchByExample() got " + example + " rescore " + rescoreWithHMM);

		if (max == 0) {
			logger.warn("max given as 0, using 10 instead...");
			max = 10;
		}

		if (start > max) { start = max - 1; }

		if (example != null && example.getEntities() != null && example.getEntities().size() < 1) {
			return null;
		}

		// (1) extract node ids from descriptors
		// (2) shortlist (find promising connected subgraphs)
		// (3) score based on transactions

    logger.debug("found "+example);
    logger.debug("found "+example.getEntities().size());
		List<FL_PatternSearchResult> results;
			results = getShortlist(example, DEFAULT_SHORT_LIST_SIZE);

		logger.debug("shortlist size = " + results.size());

		// get edges (to use in a couple places)
		List<String> exemplarIDs = null;
		try {
			exemplarIDs = getExemplarIDs(example);
		} catch (Exception e) {
			e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
		}

		List<List<Edge>> resultEdges = new ArrayList<List<Edge>>();
		// need to get nodes ids for each result in same order as associated query nodes
		List<List<String>> resultIDs = new ArrayList<List<String>>();
		if (results == null) {
			logger.error("huh? couldn't get results for " + example);
			return null;
		}
		logger.debug("searchByExample : exemplarIDs = " + exemplarIDs);
		for (FL_PatternSearchResult result : results) {
			try {
				List<Edge> allLinks = getEdgesForResult(result);
				resultEdges.add(allLinks);
				List<String> ids = getOrderedIDsForResult(result, example, exemplarIDs);
			//  logger.debug("result ids = " + ids + " for " + result);
				resultIDs.add(ids);
			} catch (Exception e) {
				e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
			}
		}

    logMemory();

		// re-score results
		if (rescoreWithHMM) {
      List<Edge> queryEdges = getAllLinks(exemplarIDs, startTime, endTime);

      if (!queryEdges.isEmpty()) {
        List<FL_PatternSearchResult> tempResults = rescoreWithHMM1(example, results, exemplarIDs, resultEdges, resultIDs, startTime, endTime);
        logger.debug("Got " + tempResults.size() + " rescored results.");
		   	results = tempResults;
      }
      else {
        logger.warn("exemplars are not connected for time period " + exemplarIDs + " start " + new Date(startTime) + " end " + new Date(endTime));
      }
		} else {
			// add all edges
			// replace links in each subgraph with aggregate links
			for (int i = 0; i < results.size(); i++) {
				List<FL_LinkMatchResult> linkMatchResults = createAggregateLinks(example, results.get(i), resultEdges.get(i));
				results.get(i).setLinks(linkMatchResults);
			}
		}

		// save mapping from results to edges
		// Map<FL_PatternSearchResult, List<Edge>> resultToEdges = new HashMap<FL_PatternSearchResult, List<Edge>>();
		// for (int i = 0; i < results.size(); i++) {
		// resultToEdges.put(results.get(i), resultEdges.get(i));
		// }

		// sort and package up results
		FL_PatternSearchResults patternSearchResults = makePatternSearchResults(results, max);

		// replace links in each subgraph with aggregate links (but only for actual results being returned
		// for (FL_PatternSearchResult patternSearchResult : patternSearchResults.getResults()) {
		// addAggregateLinks(example, patternSearchResult, resultToEdges.get(patternSearchResult));
		// }

		logger.debug(results.size() + " results, took " + (System.currentTimeMillis() - then) + " millis");
    logMemory();

    return patternSearchResults;
	}

  /**
   * @see #searchByExample(influent.idl.FL_PatternDescriptor, String, long, long, boolean, long, long)
   * @param example
   * @param results
   * @param exemplarIDs
   * @param resultEdges
   * @param resultIDs
   * @param startTime
   * @param endTime
   * @return
   */
  private List<FL_PatternSearchResult> rescoreWithHMM1(FL_PatternDescriptor example,
                                                       List<FL_PatternSearchResult> results,
                                                       List<String> exemplarIDs, List<List<Edge>> resultEdges,
                                                       List<List<String>> resultIDs, long startTime, long endTime) {
    List<Edge> queryEdges = getAllLinks(exemplarIDs, startTime, endTime);
    if (logger.isDebugEnabled()) {
      logger.debug("queryEdges = ");
      for (Edge edge : queryEdges) {
        logger.debug(edge);
      }
    }

    List<List<VectorObservation>> relevantObservations = rescoreWithHMM(results, exemplarIDs, queryEdges, resultEdges, resultIDs);
    // add only relevant edges and remove results that don't have any...
    List<FL_PatternSearchResult> tempResults = new ArrayList<FL_PatternSearchResult>();
    for (int i = 0; i < results.size(); i++) {
      List<VectorObservation> observations = relevantObservations.get(i);
      if (observations.isEmpty()) {
        continue;
      } else {
        // replace links in each subgraph (subsequence) with aggregate links (but only ones in relevant observations)
        List<Edge> relevantEdges = new ArrayList<Edge>();

        for (VectorObservation observation : observations) {
          relevantEdges.addAll(observation.getEdges());
        }
        FL_PatternSearchResult result = results.get(i);

        // aggregate links across all states
        List<FL_LinkMatchResult> linkMatchResults = createAggregateLinks(example, result, relevantEdges);
        result.setLinks(linkMatchResults);

        // aggregate links in individual states, e.g., [1], [2, 2], [3]
        // group all edges from observations in current state (and next state(s) if same state)
        int index = 0;
        String currentState = observations.get(0).getState();
        List<Edge> edges = new ArrayList<Edge>();
        while (index < observations.size()) {
          VectorObservation observation = observations.get(index);
          if (observation.getState().equalsIgnoreCase(currentState)) {
            // add more edges for current state
            edges.addAll(observation.getEdges());
          } else {
            // save edges for last state
            linkMatchResults = createAggregateLinks(example, result, edges);
            ((PatternSearchResultWithState) result).getPhaseLinks().add(linkMatchResults);
            ((PatternSearchResultWithState) result).getStates().add(currentState);

            // update to new state
            currentState = observation.getState();
            edges.clear();
            edges.addAll(observation.getEdges());
          }
          index++;
        }

        if (!edges.isEmpty()) {
          // save last state
          linkMatchResults = createAggregateLinks(example, result, edges);
          ((PatternSearchResultWithState) result).getPhaseLinks().add(linkMatchResults);
          ((PatternSearchResultWithState) result).getStates().add(currentState);
        }

        tempResults.add(result);
      }
    }
    return tempResults;
  }

  public void addRelevantEdges(FL_PatternSearchResult result, List<VectorObservation> observations) {
//		FL_LinkMatchResult linkMatchResult = new FL_LinkMatchResult();
//        FL_Link link = new FL_Link();
//        link.setSource(source);
//        link.setTarget(target);
//        link.setTags(new ArrayList<FL_LinkTag>());
//        List<FL_Property> properties = new ArrayList<FL_Property>();
//        properties.add(createProperty("numEdges", numEdges, FL_PropertyType.LONG));
//        properties.add(createProperty("outFlow", out, FL_PropertyType.DOUBLE));
//        properties.add(createProperty("inFlow", in, FL_PropertyType.DOUBLE));
//        properties.add(createProperty("netFlow", net, FL_PropertyType.DOUBLE));
//        link.setProperties(properties);
//        linkMatchResult.setLink(link);
//        linkMatchResult.setScore(1.0);
//        linkMatchResult.setUid("");
//        linkMatchResults.add(linkMatchResult);

		List<FL_LinkMatchResult> linkMatchResults = new ArrayList<FL_LinkMatchResult>();

		for (VectorObservation observation : observations) {
			for (Edge edge : observation.getEdges()) {

			}
		}

		result.setLinks(linkMatchResults);
	}

	public List<Edge> getEdgesForResult(FL_PatternSearchResult result) {
		List<FL_EntityMatchResult> entities = result.getEntities();
		List<String> entityIDs = getEntityIDs(entities);
		// logger.debug("result = " + result.toString());
		// logger.debug("entityIDs = " + entityIDs);
		return getAllLinks(entityIDs);
	}

	/**
	 * @return Result node IDs in same order as associated exemplar (query) node IDs.
	 */
	public List<String> getOrderedIDsForResult(FL_PatternSearchResult result, FL_PatternDescriptor example,
			List<String> exemplarIDs) {
		// Note: just put exemplarIDs in there to get it to be correct size
		List<String> ids = new ArrayList<String>(exemplarIDs);

		Map<String, Integer> exemplarToIndex = new HashMap<String, Integer>();
		Map<String, Integer> uidToIndex = new HashMap<String, Integer>();

		for (int i = 0; i < exemplarIDs.size(); i++) {
			exemplarToIndex.put(exemplarIDs.get(i), i);
		}

		for (FL_EntityMatchDescriptor entity : example.getEntities()) {
			String uid = entity.getUid();
			String id = entity.getExamplars().get(0);
			int index = exemplarToIndex.get(id);
			uidToIndex.put(uid, index);
		}

		for (FL_EntityMatchResult entityMatchResult : result.getEntities()) {
			String resultUid = entityMatchResult.getEntity().getUid();
			String queryUid = entityMatchResult.getUid();
			int index = uidToIndex.get(queryUid);
			ids.set(index, resultUid);
		}

		return ids;
	}

	/**
	 * Creates aggregate FL_LinkMatchResults for subgraph and sets these as links.
	 */
	protected abstract List<FL_LinkMatchResult> createAggregateLinks(FL_PatternDescriptor example, FL_PatternSearchResult result,
			List<Edge> edges);

	/**
	 * Retrieves promising (somewhat) connected subgraphs based on node similarity.
   * @see #searchByExample(influent.idl.FL_PatternDescriptor, String, long, long, boolean, long, long)
	 */
	private List<FL_PatternSearchResult> getShortlist(FL_PatternDescriptor example, long max) {
		// (2a) retrieve list of nodes for each query node ranked by similarity
		// (2b) form groups across lists from prioritized Cartesian product
		// (2c) filter by activity (i.e., subgraph must be connected)

		List<String> exemplarIDs = getExemplarIDs(example);
    List<FL_EntityMatchDescriptor> objects = Collections.emptyList();
    List<FL_EntityMatchDescriptor> entities1 = example != null ? example.getEntities() : objects;

   // return getShortlist(entities1, exemplarIDs, max);
    return getShortlistFast(entities1,exemplarIDs,max);
  }


  private List<FL_PatternSearchResult> getShortlistFast(List<FL_EntityMatchDescriptor> entities1, List<String> exemplarIDs,
                                                        long max) {
    int k = (int) (max/1);
    boolean skipSelf = SKIP_SELF_AS_NEIGHBOR;

    long then = System.currentTimeMillis();
    SortedSet<CandidateGraph> candidates = new TreeSet<CandidateGraph>();

    String firstExemplar = exemplarIDs.iterator().next();
    List<String> neighbors = getNearestNeighbors(firstExemplar, k, skipSelf);

  //  candidates.add(new CandidateGraph(exemplarIDs, firstExemplar, k));

    for (String node : neighbors) {
      if (stot.containsKey(node)) {
        candidates.add(new CandidateGraph(exemplarIDs, node, k));
      }
    }

    if (!candidates.isEmpty()) {
     // logger.debug("depth 1 : " + candidates.size() + " best " + candidates.first() + " worst " + candidates.last());
    }
    else {
       logger.debug("depth 1 : " + candidates.size() );

    }

    for (int i = 1; i < exemplarIDs.size(); i++) {
      SortedSet<CandidateGraph> nextCandidates = new TreeSet<CandidateGraph>();
      for (CandidateGraph candidateGraph : candidates) {
        candidateGraph.makeNextGraphs2(nextCandidates, MAX_CANDIDATES);

       /* if (!nextCandidates.isEmpty()) {
          logger.debug("depth " + i +
              " : " + nextCandidates.size() + " best " + nextCandidates.first() + " worst " + nextCandidates.last());
        }*/
      }

      candidates = nextCandidates;
 /* if (!candidates.isEmpty()) {
        logger.debug("depth " + i +
            " : " + candidates.size() + " best " + candidates.first() + " worst " + candidates.last());
      }*/
    }

    if (!candidates.isEmpty()) {
      logger.debug("getShortlistFast : " + candidates.size() + " best " + candidates.first() + " worst " + candidates.last());
    }

    List<FL_PatternSearchResult> results = new ArrayList<FL_PatternSearchResult>();
    int count = 0;
    CandidateGraph queryGraph = new CandidateGraph(exemplarIDs, firstExemplar, 10).makeDefault();
    boolean found = false;
    for (CandidateGraph graph : candidates) {
      if (graph.equals(queryGraph)) { found = true; break; }
    }
    if (!found) candidates.add(queryGraph);

    for (CandidateGraph graph : candidates) {
      List<FL_EntityMatchResult> entities = new ArrayList<FL_EntityMatchResult>();

      List<String> nodes = graph.getNodes();
      for (int i = 0; i < exemplarIDs.size(); i++) {
        String similarID = nodes.get(i);
        double similarity = getSimilarity(exemplarIDs.get(i), similarID);
        String exemplarQueryID = entities1.get(i).getUid();
        if (queryGraph.getScore() > ((float)exemplarIDs.size())-0.1) {
          logger.debug("\t graph " + graph + " got " + similarID + " and " + exemplarIDs.get(i));
        }
        FL_EntityMatchResult entityMatchResult = makeEntityMatchResult(exemplarQueryID, similarID, similarity);
        if (entityMatchResult != null) {
          entities.add(entityMatchResult);
        }
      }

      // arithmetic mean
      double score = getSimpleScore(entities);
      boolean query = graph == queryGraph;
      FL_PatternSearchResult result = makeResult(entities, score, query);
      if (query) logger.debug("\n\n\n\n found query!!! " + result);
      results.add(result);

     /* count++;
      if (count >= max+1) {
        break;
      }*/
    }

    long now = System.currentTimeMillis();
    logger.debug("getShortlistFast took " + (now-then) + " millis to get " + results.size() + " candidates");

    return results;
  }

  protected void logMemory() {
    Runtime rt = Runtime.getRuntime();
    long free = rt.freeMemory();
    long used = rt.totalMemory() - free;
    long max = rt.maxMemory();
    logger.debug("heap info free " + free / MB + "M used " + used / MB + "M max " + max / MB + "M");
  }


  private class CandidateGraph implements Comparable<CandidateGraph> {
    private static final int MAX_NEIGHBORS = 1000;
    private final List<String> exemplars;
    private List<String> nodes = new ArrayList<String>();
    private float score;
    int k;
    CandidateGraph(CandidateGraph toCopy) {
      exemplars = toCopy.exemplars;
      nodes = new ArrayList<String>(toCopy.getNodes());
      this.k = toCopy.k;
      this.score = toCopy.score;
    }

    CandidateGraph(List<String> exemplars, String initial, int k) {
      this.exemplars = exemplars;
      addNode(initial);
      this.k = k;
    }
    public CandidateGraph makeDefault() {
      for (String exemplar : exemplars){
        if (!nodes.contains(exemplar)) nodes.add(exemplar);
      }
      return this;
    }
    public boolean wouldBeConnected(String nodeid) {
      boolean connected = false;
      try {
        for (String currentNode : nodes) {
          connected = isPairConnected(currentNode, nodeid);
          if (connected) break;
        }
      } catch (Exception e) {
      }
      return connected;
    }
/*    private List<CandidateGraph> makeNextGraphs() {
      List<String> neighbors = getNearestNeighbors(nodes.get(nodes.size() - 1), k, SKIP_SELF_AS_NEIGHBOR);

      List<CandidateGraph> nextHopGraphs = new ArrayList<CandidateGraph>();
      for (String nextHopNode : neighbors) {
        if (!nodes.contains(nextHopNode) && wouldBeConnected(nextHopNode) && validTargets.contains(nextHopNode)) {
          CandidateGraph candidateGraph = new CandidateGraph(this);
          candidateGraph.addNode(nextHopNode);
          nextHopGraphs.add(candidateGraph);
        }
      }
      return nextHopGraphs;
    }*/

    public void makeNextGraphs2(SortedSet<CandidateGraph> candidates, int maxSize) {
      final String lastNodeInGraph = nodes.get(nodes.size() - 1);
      Set<String> oneHopNeighbors = stot.get(lastNodeInGraph);
      if (oneHopNeighbors == null) {
        logger.error("huh? '" + lastNodeInGraph +
            "' has no transactions?");
        return;
      }
      List<String> sortedNeighbors = new ArrayList<String>(oneHopNeighbors);
      Collections.sort(sortedNeighbors, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          double toFirst = getSimilarity(lastNodeInGraph, o1);
          double toSecond = getSimilarity(lastNodeInGraph, o2);
          return toFirst < toSecond ? +1 : toFirst > toSecond ? -1 : 0;
        }
      });

      sortedNeighbors = sortedNeighbors.subList(0,Math.min(sortedNeighbors.size(),MAX_NEIGHBORS));

      //logger.debug("this " + this + " found " + sortedNeighbors.size() + " neighbors of " + lastNodeInGraph);
      for (String nextHopNode : sortedNeighbors) {
        if (!nodes.contains(nextHopNode) && validTargets.contains(nextHopNode)) {    // no cycles!
          CandidateGraph candidateGraph = new CandidateGraph(this);
          candidateGraph.addNode(nextHopNode);
          if (candidates.size() < maxSize || candidateGraph.getScore() > candidates.last().getScore()) {
            candidates.add(candidateGraph);
          }
        }
      }
    }

    private void addNode(String nodeid) {
      String compareAgainst = exemplars.get(getNodes().size());
      double similarity = getSimilarity(compareAgainst, nodeid);
      score += similarity;
      nodes.add(nodeid);
    }

    @Override
    public int compareTo(CandidateGraph o) {
      return o.getScore() < getScore() ? -1 : o.getScore() > getScore() ? +1 : 0;
    }

    public List<String> getNodes() {
      return nodes;
    }

    public float getScore() {
      return score;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CandidateGraph) {
        CandidateGraph o = (CandidateGraph) obj;
        return nodes.equals(o.getNodes());
      }
      else return false;
    }

    public String toString() {
      boolean isQuery = getNodes().equals(exemplars);
       return (isQuery ? " QUERY " : "")+" nodes " + getNodes() + " score " + getScore();
    }
  }

  /**
   * @see #getShortlist(influent.idl.FL_PatternDescriptor, long)
   * @param entities1
   * @param exemplarIDs
   * @param max
   * @return
   * @deprecated
   */
  private List<FL_PatternSearchResult> getShortlist(List<FL_EntityMatchDescriptor> entities1, List<String> exemplarIDs,
                                                    long max) {
    long then = System.currentTimeMillis();
    // TODO : skip including self in list?
    boolean skipSelf = SKIP_SELF_AS_NEIGHBOR;
    Map<String, List<String>> idToNeighbors = new HashMap<String, List<String>>();

    int k = (int) (100 * max);
    for (String id : exemplarIDs) {
      List<String> neighbors = getNearestNeighbors(id, k, skipSelf);
      if (neighbors.size() == 0) {
        logger.warn("no neighbors for " + id + " among nearest " +k+ "???\n\n");
        return Collections.emptyList();
      }
      idToNeighbors.put(id, neighbors);
    }

    logger.debug("for " + exemplarIDs.size() + " examples, took " + (System.currentTimeMillis() - then)
        + " millis to find neighbors");

    int[] listSizes = new int[exemplarIDs.size()];
    for (int i = 0; i < exemplarIDs.size(); i++) {
      listSizes[i] = idToNeighbors.get(exemplarIDs.get(i)).size();
    }
    PrioritizedCartesianProduct product = new PrioritizedCartesianProduct(listSizes);

    long maxTries = max + MAX_TRIES;
    long numTries = 0;
    int count = 0;
    List<FL_PatternSearchResult> results = new ArrayList<FL_PatternSearchResult>();

    // TODO : grab more than max since we're going to re-sort with a refined subgraph score?
    while (product.next()) {
      numTries++;

      if (numTries % 100000 == 0) {
        logger.debug("numTries = " + numTries + " / maxTries = " + maxTries);
      }

      if (numTries >= maxTries) {
        logger.debug("reached maxTries = " + maxTries);
        break;
      }

      int[] indices = product.getIndices();

      List<String> subgraphIDs = new ArrayList<String>();
      for (int i = 0; i < indices.length; i++) {
        String exemplarID = exemplarIDs.get(i);
        String similarID = idToNeighbors.get(exemplarID).get(indices[i]);
        subgraphIDs.add(similarID);
      }

      // skip if any node id repeated
      Set<String> idSet = new HashSet<String>(subgraphIDs);
      if (idSet.size() < subgraphIDs.size()) {
        continue;
      }

      // skip if not somewhat connected
      if (!connectedGroup(subgraphIDs)) {
        continue;
      }

      List<FL_EntityMatchResult> entities = getEntitiesFromIds(entities1, exemplarIDs, idToNeighbors, indices);

      // arithmetic mean
      double score = getSimpleScore(entities);
      FL_PatternSearchResult result = makeResult(entities, score, false);
      results.add(result);

      count++;
      if (count >= max) {
        break;
      }
    }

    //logger.debug("numTries = " + numTries + " results " + results.size());

    return results;
  }

  private List<FL_EntityMatchResult> getEntitiesFromIds(List<FL_EntityMatchDescriptor> entities1,
                                                        List<String> exemplarIDs,
                                                        Map<String, List<String>> idToNeighbors, int[] indices) {
    List<FL_EntityMatchResult> entities = new ArrayList<FL_EntityMatchResult>();
    for (int i = 0; i < indices.length; i++) {
      String exemplarQueryID = entities1.get(i).getUid();

      String exemplarID = exemplarIDs.get(i);
      String similarID = idToNeighbors.get(exemplarID).get(indices[i]);
      double similarity = getSimilarity(exemplarID, similarID);
      FL_EntityMatchResult entityMatchResult = makeEntityMatchResult(exemplarQueryID, similarID, similarity);
      if (entityMatchResult != null) {
        entities.add(entityMatchResult);
      }
    }
    return entities;
  }

  public FL_EntityMatchResult makeEntityMatchResult(String queryID, String resultID, double score) {
		FL_EntityMatchDescriptor descriptor = new FL_EntityMatchDescriptor();
		// reuse Uid from query
		descriptor.setUid(queryID);
		descriptor.setEntities(Arrays.asList(resultID));
		ResultInfo resultInfo = getEntitiesByID(descriptor);

    if (resultInfo.rows.isEmpty()) return null;
		// should only get 1!
		Map<String, String> entityMap = resultInfo.rows.get(0);

		FL_EntityMatchResult entityMatchResult = makeEntityMatchResult(descriptor, resultInfo, entityMap);
		// make sure entity id still has prefix
		entityMatchResult.getEntity().setUid(resultID);
		entityMatchResult.setScore(score);

		return entityMatchResult;
	}

	public static List<String> split(String s, String separator) {
		List<String> fields = new ArrayList<String>();
		int i = 0;
		// add fields up to last separator
		while (i < s.length()) {
			int index = s.indexOf(separator, i);
			if (index < 0) {
				break;
			}
			fields.add(s.substring(i, index));
			i = index + 1;
		}
		// add field after last separator
		fields.add(s.substring(i, s.length()));
		return fields;
	}

	/**
	 * Rescores results by comparing result subgraph's transactions to query graph's transactions.
	 */
/*	private void rescoreWithHMM_OLD(FL_PatternDescriptor query, List<FL_PatternSearchResult> results,
			List<String> exemplarIDs, List<Edge> queryEdges, List<List<Edge>> resultEdges) {
		//
		// get edges and features for query subgraph and result subgraphs
		//

		List<Transaction> queryTransactions = createFeatureVectors(queryEdges, exemplarIDs);
		List<List<Transaction>> resultTransactions = new ArrayList<List<Transaction>>();
		for (List<Edge> edges : resultEdges) {
			resultTransactions.add(createFeatureVectors(edges, exemplarIDs));
		}

		//
		// normalize features
		//

		// pack features from query and results
		int numDimensions = queryTransactions.get(0).getFeatures().length;
		int numSamples = queryTransactions.size();
		for (List<Transaction> transactions : resultTransactions) {
			numSamples += transactions.size();
		}
		double[][] rawFeatures = new double[numSamples][numDimensions];
		int index = 0;
		for (Transaction transaction : queryTransactions) {
			rawFeatures[index++] = transaction.getFeatures();
		}
		for (List<Transaction> transactions : resultTransactions) {
			for (Transaction transaction : transactions) {
				rawFeatures[index++] = transaction.getFeatures();
			}
		}

		double lowerPercentile = 0.025;
		double upperPercentile = 0.975;
		FeatureNormalizer normalizer = new FeatureNormalizer(rawFeatures, lowerPercentile, upperPercentile);
		double[][] stdFeatures = normalizer.normalizeFeatures(rawFeatures);

		// unpack features
		index = 0;
		for (Transaction transaction : queryTransactions) {
			transaction.setFeatures(stdFeatures[index++]);
		}
		for (List<Transaction> transactions : resultTransactions) {
			for (Transaction transaction : transactions) {
				transaction.setFeatures(stdFeatures[index++]);
			}
		}

		//
		// "train" HMM
		//

		long hour = 3600L * 1000L;
		long day = 24L * hour;
		long week = 7L * day;
		long timeThresholdMillis = 1L * hour;
		logger.debug("constructing HMM");
		HmmScorer scorer = new HmmScorer(queryTransactions, timeThresholdMillis, HMM_KDE_BANDWIDTH);

		//
		// score result subgraphs
		//

		logger.debug("scoring subgraphs");
		double queryScore = scorer.score(queryTransactions);
		for (int i = 0; i < results.size(); i++) {
			double score = scorer.score(resultTransactions.get(i));
			// NOTE: this also penalizes scores that are "better" than the queryScore
			double distance = Math.abs(score - queryScore);
			double similarity = 1.0 / (1.0 + HMM_SCALE_DISTANCE * distance);
			results.get(i).setScore(similarity);
		}

		// write out edge attributes and features to console
		// try {
		// PrintWriter pw = new PrintWriter("c:/temp/edges_" + System.currentTimeMillis() + ".tsv");
		// for (int i = 0; i < results.size(); i++) {
		// pw.println(AvroUtils.toString(results.get(i)));
		// List<Edge> edges = resultEdges.get(i);
		// List<Transaction> transactions = resultTransactions.get(i);
		// for (int j = 0; j < edges.size(); j++) {
		// pw.print(edges.get(j));
		// pw.print("\t");
		// pw.print(transactions.get(j).featuresToString("\t"));
		// pw.println();
		// }
		// pw.println();
		// }
		// pw.close();
		// } catch (FileNotFoundException e) {
		// e.printStackTrace();
		// }
	}*/

	/**
	 * Rescores results by comparing result subgraph's transactions to query graph's transactions.
	 */
	private List<List<VectorObservation>> rescoreWithHMM(List<FL_PatternSearchResult> results,
			List<String> exemplarIDs, List<Edge> queryEdges, List<List<Edge>> resultEdges, List<List<String>> resultIDs) {
		//
		// get edges and features for query subgraph and result subgraphs
		//

    logger.debug("rescoreWithHMM got " + results.size() + " initial results to rescore");


		//
		// normalize features
		//

		// pack features from query and results
    List<VectorObservation> queryObservations = createObservationVectors(queryEdges, exemplarIDs);
		int numSamples = queryObservations.size();
    List<List<VectorObservation>> resultObservations = getResultObservations(resultEdges, resultIDs, exemplarIDs);
		for (List<VectorObservation> observations : resultObservations) {
			numSamples += observations.size();
		}
    int numDimensions = queryObservations.get(0).getValues().length;
    logger.debug("query observations " + queryObservations.size() + " samples " + numSamples + " num dim " + numDimensions);
    double[][] rawFeatures = getRawFeatures(resultObservations, queryObservations, numSamples, numDimensions);
    int index;

		// logger.debug("pre-normalized features");
		// for (List<VectorObservation> observations : resultObservations) {
		// logger.debug("for result:");
		// for (VectorObservation observation : observations) {
		// logger.debug("   " + Arrays.toString(observation.getValues()));
		// }
		// }

    double[][] stdFeatures = getStandardFeatures(numSamples, numDimensions, rawFeatures);

		// unpack features
		index = 0;
		for (VectorObservation observation : queryObservations) {
			observation.setValues(stdFeatures[index++]);
		}
		for (List<VectorObservation> observations : resultObservations) {
			for (VectorObservation observation : observations) {
				observation.setValues(stdFeatures[index++]);
			}
		}

		// logger.debug("normalized features");
		// logger.debug("query:");
		// for (VectorObservation observation : queryObservations) {
		// logger.debug("   " + Arrays.toString(observation.getValues()));
		// }
		// index = 0;
		// for (List<VectorObservation> observations : resultObservations) {
		// logger.debug("for result: " + resultIDs.get(index++));
		// for (VectorObservation observation : observations) {
		// logger.debug("   " + Arrays.toString(observation.getValues()));
		// }
		// }
		
		//
		// "train" HMM
		//

		// double[][] A = new double[4][];
		// A[0] = new double[] { 0.0, 1.0, 0.0, 0.0 };
		// A[1] = new double[] { 0.0, 0.5, 0.5, 0.0 };
		// A[2] = new double[] { 0.0, 0.0, 0.5, 0.5 };
		// A[3] = new double[] { 0.0, 0.0, 0.0, 0.0 };

		// TODO: assign buckets to phases/states through query parameters or infer them?
		// assumption: 1 phase per bucket?
    Hmm<VectorObservation> hmm = makeHMM(queryObservations);

		//
		// score result subgraphs
		//

		//logger.debug("scoring query");
		List<StateSequence> sequences = hmm.decodeTopKLog(queryObservations, 3);
		StateSequence sequence = sequences.get(0);
		double queryScore = sequence.getScore();
		logger.debug("query: raw score = " + queryScore + "; start index = " + sequence.getStartIndex() + "; states = " + sequence.getStates());

		//logger.debug("scoring subgraphs");
		// return the relevant observations given optimal subsequences
		List<List<VectorObservation>> relevantObservations = new ArrayList<List<VectorObservation>>();

    double bestSoFar = 0;
    int skipped = 0;
    for (int i = 0; i < results.size(); i++) {
      if (resultObservations.get(i).isEmpty()) {
        if (exemplarIDs.equals(resultIDs.get(i)))

          logger.debug("NO OBSERVATIONS for result i = " + i + "(" + resultIDs.get(i) +
              "): num edges = " + resultEdges.get(i).size());

        results.get(i).setScore(Double.NEGATIVE_INFINITY);
        relevantObservations.add(new ArrayList<VectorObservation>());
        skipped++;
        continue;
      }
      sequences = hmm.decodeTopKLog(resultObservations.get(i), 3);
      if (sequences.isEmpty()) {
        if (exemplarIDs.equals(resultIDs.get(i)))
          logger.warn("----> not enough data for result i = " + i + " (" + resultIDs.get(i) +
              "): num edges = " + resultEdges.get(i).size() + ", num observations = " + resultObservations.get(i).size());
      /*  else {
          logger.debug("not enough data for result i = " + i + " (" + resultIDs.get(i) +
              "): num edges = " + resultEdges.get(i).size() + ", num observations = " + resultObservations.get(i).size());
        }*/
        results.get(i).setScore(Double.NEGATIVE_INFINITY);
        relevantObservations.add(new ArrayList<VectorObservation>());
        continue;
      }
      sequence = sequences.get(0);
      double score = sequence.getScore();
      logger.debug("ids = " + resultIDs.get(i) + ", raw score = " + score + "; start index = " + sequence.getStartIndex() + "; states = " + sequence.getStates());
      //logger.debug("other scores:");
      for (StateSequence other : sequences.subList(1, sequences.size())) {
        logger.debug("   raw score = " + other.getScore() + "; start index = " + other.getStartIndex() + "; states = " + other.getStates());
      }
      // NOTE: this also penalizes scores that are "better" than the queryScore
      double distance = Math.abs(score - queryScore);
      double similarity = 1.0 / (1.0 + HMM_SCALE_DISTANCE * distance);
      if (similarity > bestSoFar) {
        bestSoFar = similarity;
			  logger.debug("similarity = " + similarity);
      }
			results.get(i).setScore(similarity);
			// TODO: pack states into VectorObservation objects (so caller has access to associations)
			int start = sequence.getStartIndex();
			int end = start + sequence.getStates().size();
			List<VectorObservation> temp = resultObservations.get(i).subList(start, end);
			// add state information
			for (int j = 0; j < temp.size(); j++) {
				temp.get(j).setState("" + sequence.getStates().get(j));
			}
			relevantObservations.add(temp);
		}

    if (skipped > 0)logger.debug("rescoreWithHMM skipped " + skipped + " num observations " + relevantObservations.size());

		return relevantObservations;
	}

  private double[][] getStandardFeatures(int numSamples, int numDimensions, double[][] rawFeatures) {
    double lowerPercentile = 0.025;
    double upperPercentile = 0.975;
    FeatureNormalizer normalizer = new FeatureNormalizer(rawFeatures, lowerPercentile, upperPercentile);
    double[][] stdFeatures = normalizer.normalizeFeatures(rawFeatures);

    // replace NaN's with zeros?
    double replacement = 0.0;
    for (int i = 0; i < numSamples; i++) {
      for (int j = 0; j < numDimensions; j++) {
        if (Double.isNaN(stdFeatures[i][j])) {
          stdFeatures[i][j] = replacement;
        }
      }
    }
    return stdFeatures;
  }

  private double[][] getRawFeatures(List<List<VectorObservation>> resultObservations, List<VectorObservation> queryObservations, int numSamples, int numDimensions) {
    double[][] rawFeatures = new double[numSamples][numDimensions];
    int index = 0;
    for (VectorObservation observation : queryObservations) {
      rawFeatures[index++] = observation.getValues();
    }
    for (List<VectorObservation> observations : resultObservations) {
      for (VectorObservation observation : observations) {
        rawFeatures[index++] = observation.getValues();
      }
    }
    return rawFeatures;
  }

  private Hmm<VectorObservation> makeHMM(List<VectorObservation> queryObservations) {
    int numPhases = queryObservations.size();
    double selfLoop = 0.5;
    double[][] A = new double[numPhases + 2][];
    A[0] = new double[numPhases + 2];
    A[0][1] = 1.0; // always go from q_0 to state 1
    for (int i = 1; i <= numPhases; i++) {
      A[i] = new double[numPhases + 2];
      A[i][i] = selfLoop;
      A[i][i + 1] = 1.0 - selfLoop;
    }
    A[numPhases + 1] = new double[numPhases + 2];
    logger.debug("numPhases = " + numPhases);
    logger.debug("A = " + Arrays.deepToString(A));

    List<ObservationLikelihood<VectorObservation>> b = new ArrayList<ObservationLikelihood<VectorObservation>>();
    for (int i = 0; i < numPhases; i++) {
      // b.add(new KernelDensityLikelihood(queryObservations.subList(i, i + 1), HMM_KDE_BANDWIDTH));
      b.add(new KernelDensityLikelihood(queryObservations.subList(i, i + 1), 0.75));
    }

    return new Hmm<VectorObservation>(A, b);
  }

  /**
   * @see #rescoreWithHMM(java.util.List, java.util.List, java.util.List, java.util.List, java.util.List)
   * @param resultEdges
   * @param resultIDs
   * @param exemplarIDs
   * @return
   */
  private List<List<VectorObservation>> getResultObservations(List<List<Edge>> resultEdges, List<List<String>> resultIDs, List<String> exemplarIDs) {
    List<List<VectorObservation>> resultObservations = new ArrayList<List<VectorObservation>>();
    for (int i = 0; i < resultEdges.size(); i++) {
      if (!resultEdges.get(i).isEmpty()) {
        List<VectorObservation> observationVectors = createObservationVectors(resultEdges.get(i), resultIDs.get(i));
        resultObservations.add(observationVectors);
         if (resultIDs.get(i).equals(exemplarIDs)) {
           logger.debug("\n\n result observations for exemplar " + observationVectors.size() + " at " + i);
         }
      } else {
        List<VectorObservation> objects = Collections.emptyList();
        resultObservations.add(objects);
      }
    }
    return resultObservations;
  }

  /**
   * Sorts scores and packages into FL_PatternSearchResults.
	 */
	private FL_PatternSearchResults makePatternSearchResults(List<FL_PatternSearchResult> results, long max) {
		//logger.debug("ENTER makePatternSearchResults()");

		Comparator<FL_PatternSearchResult> comparator = new Comparator<FL_PatternSearchResult>() {
			@Override
			public int compare(FL_PatternSearchResult result1, FL_PatternSearchResult result2) {
				return result1.getScore().compareTo(result2.getScore());
			}
		};
		Collections.sort(results, Collections.reverseOrder(comparator));

		if (results.size() > max) {
			results = results.subList(0, (int) max);
		}

		FL_PatternSearchResults patternSearchResults = new FL_PatternSearchResults();
		patternSearchResults.setResults(results);
		patternSearchResults.setTotal((long) results.size());

	//	logger.debug("EXIT makePatternSearchResults()");
		return patternSearchResults;
	}

	private double getSimpleScore(List<FL_EntityMatchResult> entities) {
		double sum = 0.0;
		for (FL_EntityMatchResult entityMatchResult : entities) {
			sum += entityMatchResult.getScore();
		}
		return sum / entities.size();
	}

	private FL_PatternSearchResult makeResult(List<FL_EntityMatchResult> entities, double score, boolean isQuery) {
		// FL_PatternSearchResult result = new FL_PatternSearchResult();
		FL_PatternSearchResult result = new PatternSearchResultWithState(isQuery);
		result.setEntities(entities);
		List<FL_LinkMatchResult> links = getLinks(entities);
		result.setLinks(links);
		result.setScore(score);
		return result;
	}

	/**
	 * @return true if pair of nodes are connected
	 */
	protected abstract boolean isPairConnected(String id1, String id2) throws Exception;

	/**
	 * @see #searchByExample(influent.idl.FL_PatternDescriptor, String, long, long)
	 * @return true if each node connected to at least one other node
	 */
	private boolean connectedGroup(List<String> ids) {
		// TODO : verify that group is actually connected?

		//long then = System.currentTimeMillis();

		if (ids.size() == 1) {
			return true;
		}

		try {
			for (int i = 0; i < ids.size(); i++) {
				boolean connected = false;
				for (int j = 0; j < ids.size(); j++) {
					if (j == i) {
						continue;
					}
					connected = isPairConnected(ids.get(i), ids.get(j));
					if (connected) {
						break;
					}
				}
				if (!connected) {
					return false;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		// logger.debug("connected : took "+ (System.currentTimeMillis()-then) + " millis to check " +ids.size());
		return true;
	}

	/**
	 * @return key into edge metadata table for retrieving metadata on edge between pair of nodes
	 *
	 *         Note: If all metadata already in edge table, then returns edge/transaction id from edge table.
	 */
	protected abstract String getEdgeMetadataKey(String id1, String id2) throws Exception;

	/**
	 * @return FL_Property for edge metadata key (key into table with additional edge attributes)
	 */
	protected abstract FL_Property createEdgeMetadataKeyProperty(String id);

	/**
	 * @return links between entities (if connected in edge_index table)
	 */
	protected List<FL_LinkMatchResult> getLinks(List<FL_EntityMatchResult> entities) {
		// NOTE : this returns at most one link per pair of nodes...

		List<FL_LinkMatchResult> linkMatchResults = new ArrayList<FL_LinkMatchResult>();

		if (entities.size() == 1) {
			return linkMatchResults;
		}

		try {
			// iterate over all pairs of entities
			for (int i = 0; i < entities.size(); i++) {
				String source = entities.get(i).getEntity().getUid();
				for (int j = i + 1; j < entities.size(); j++) {
					String target = entities.get(j).getEntity().getUid();
					String edgeMetadataKey = getEdgeMetadataKey(source, target);
					if (edgeMetadataKey != null) {
						FL_LinkMatchResult linkMatchResult = new FL_LinkMatchResult();
						FL_Link link = new FL_Link();
						link.setSource(source);
						link.setTarget(target);
						link.setTags(new ArrayList<FL_LinkTag>());
						List<FL_Property> properties = new ArrayList<FL_Property>();
						properties.add(createEdgeMetadataKeyProperty(edgeMetadataKey));
						link.setProperties(properties);
						linkMatchResult.setLink(link);
						linkMatchResult.setScore(1.0);
						linkMatchResult.setUid("");
						linkMatchResults.add(linkMatchResult);
					} else {
						// System.out.println("no edge between: " + source + " & " + target);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<FL_LinkMatchResult>();
		}

		return linkMatchResults;
	}

	/**
	 * @return all links between entities along with their metadata
	 */
	protected abstract List<Edge> getAllLinks(List<String> ids);

	/**
	 * @return all links between entities along with their metadata in [startTime, endTime]
	 */
	protected abstract List<Edge> getAllLinks(List<String> ids, long startTime, long endTime);

	/**
	 * @return feature vectors associated with subgraph's edges
	 */
	protected abstract List<Transaction> createFeatureVectors(List<Edge> edges, List<String> ids);

	// TODO: should make generic base implementation for createObservationVectors()
	/**
	 * @return feature vectors associated with subgraph's edges
	 */
	protected abstract List<VectorObservation> createObservationVectors(List<Edge> edges, List<String> ids);

	/**
	 * Gotta rename this -- represents results of a specific type -- all the lenders, for instance. If a query returned
	 * lenders and borrowers, there would be two different instances of this class.
	 */
	public static class ResultInfo {
		public Map<String, String> nameToType;
		public List<Map<String, String>> rows;
		private String table;

		public ResultInfo() {
			this.nameToType = Collections.emptyMap();
			this.rows = Collections.emptyList();
		}

		public ResultInfo(Map<String, String> nameToType, List<Map<String, String>> rows) {
			this.nameToType = nameToType;
			this.rows = rows;
			this.setTable(getTable());
		}

		public String toString() {
			return "Type " + table + " : " + rows.size() + " rows : "
					+ (rows.isEmpty() ? "" : " e.g. " + rows.iterator().next());
		}

		public String getTable() {
			return table;
		}

		public void setTable(String table) {
			this.table = table;
		}

		public boolean isEmpty() {
			return rows.isEmpty();
		}
	}

	protected static class ForeignLink {
		public String linkTable;
		public LocalToForeignKeyJoin sourcePair;
		public LocalToForeignKeyJoin targetPair;

		// LocalToForeignKeyJoin pair;

		public ForeignLink(String linkTable, // LocalToForeignKeyJoin pair) {
				String sourceJoin, String targetJoin) {
			this(linkTable, new LocalToForeignKeyJoin(sourceJoin), new LocalToForeignKeyJoin(targetJoin));

		}

		public ForeignLink(String linkTable, // LocalToForeignKeyJoin pair) {
				LocalToForeignKeyJoin sourcePair, LocalToForeignKeyJoin targetPair) {
			this.linkTable = linkTable;
			this.sourcePair = sourcePair;
			this.targetPair = targetPair;
			// this.pair = pair;
		}

		public String toString() {
			return linkTable + "->" + sourcePair + "/" + targetPair;
		}
	}

	protected static class LocalToForeignKeyJoin {
		public String entityKey;
		public String foreignKey;

		public LocalToForeignKeyJoin(String commonKey) {
			this(commonKey, commonKey);
		}

		public LocalToForeignKeyJoin(String entityKey, String foreign) {
			this.entityKey = entityKey;
			this.foreignKey = foreign;
		}
	}

	public interface Edge extends Comparable<Edge> {
		Object getSource();

		Object getTarget();

		long getTime();
	};
}
