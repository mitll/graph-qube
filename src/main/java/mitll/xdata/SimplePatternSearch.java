// Copyright 2013 MIT Lincoln Laboratory, Massachusetts Institute of Technology 
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

package mitll.xdata;

import influent.idl.FL_BoundedRange;
import influent.idl.FL_Entity;
import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_EntityTag;
import influent.idl.FL_Future;
import influent.idl.FL_PatternDescriptor;
import influent.idl.FL_PatternSearch;
import influent.idl.FL_PatternSearchResult;
import influent.idl.FL_PatternSearchResults;
import influent.idl.FL_Property;
import influent.idl.FL_PropertyTag;
import influent.idl.FL_PropertyType;
import influent.idl.FL_Service;
import influent.idl.FL_SingletonRange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import mitll.xdata.binding.Binding;
import mitll.xdata.binding.BitcoinBinding;
import mitll.xdata.binding.KivaBinding;
import mitll.xdata.binding.TestBinding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.db.MysqlConnection;

import org.apache.avro.AvroRemoteException;
import org.apache.log4j.Logger;

public class SimplePatternSearch implements FL_PatternSearch {
  private static Logger logger = Logger.getLogger(SimplePatternSearch.class);
  private static final boolean USE_H2_KIVA = false;

    private Connection connection;
    private KivaBinding kivaBinding = null;
  private BitcoinBinding bitcoinBinding = null;
  private TestBinding testBinding = null;

    /**
     * @see GraphQuBEServer#main(String[])
     * @throws Exception
     * @deprecated unused currently
     */
    private SimplePatternSearch(String database, boolean useMysql) throws Exception {
        DBConnection mysqlConnection = useMysql ? new MysqlConnection(database) : new H2Connection("kiva");
        connection = mysqlConnection.getConnection();
        kivaBinding = new KivaBinding(mysqlConnection);
    }

  /**
   *
   * @param database
   * @param username
   * @param password
   * @throws Exception
   * @deprecated unused currently
   */
  private SimplePatternSearch(String database, String username, String password) throws Exception {
        MysqlConnection mysqlConnection = new MysqlConnection(database, username, password);
        if (mysqlConnection.isValid()) {
            connection = mysqlConnection.getConnection();
        }
        kivaBinding = new KivaBinding(mysqlConnection);
    }

  public SimplePatternSearch() {}

    /**
     * Creates SimplePatternSearch that can detect Kiva vs Bitcoin ids in query.
     * 
     * Assumes that kiva.h2.db and bitcoin.h2.db are live in kivaDirectory and bitcoinDirectory.
     * 
     * @see GraphQuBEServer#main(String[])
     */
    public static SimplePatternSearch getDemoPatternSearch(String kivaDirectory, String bitcoinDirectory,
            boolean useFastBitcoinConnectedTest) throws Exception {
        SimplePatternSearch search = new SimplePatternSearch();

      if (false) {
        DBConnection dbConnection = USE_H2_KIVA ? new H2Connection(kivaDirectory, "kiva") : new MysqlConnection("kiva");
        try {
          search.kivaBinding = new KivaBinding(dbConnection);
        } catch (Exception e) {
          logger.error("got " + e, e);
        }

        dbConnection = new H2Connection(bitcoinDirectory, "bitcoin");
        search.bitcoinBinding = new BitcoinBinding(dbConnection, useFastBitcoinConnectedTest);
      }
      search.testBinding = new TestBinding();

      return search;
    }

    /*
     * public SimplePatternSearch(String jdbcURL, String username, String password) throws Exception {
     * Class.forName("org.h2.Driver"); connection = DriverManager.getConnection(jdbcURL, username, password); }
     */

    @Override
    public Void setTimeout(FL_Future future, long timeout) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean getCompleted(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getError(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public double getProgress(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getExpectedDuration(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Void stop(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<FL_Future> getFutures() throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    public Binding getBinding(FL_PatternDescriptor example) {
        boolean useKiva = useKiva(example);

        logger.debug("for " + example + " use kiva : " + useKiva);
        if (useKiva && kivaBinding != null) {
            return kivaBinding;
        } else if (bitcoinBinding != null) {
            return bitcoinBinding;
        }
        return null;
    }

  public Binding getTestBinding() { return testBinding; }

    @Override
    public Object searchByExample(FL_PatternDescriptor example, String service, long start, long max,
            FL_BoundedRange dateRange) throws AvroRemoteException {
        // TODO : support dateRange
        return searchByExample(example, service, start, max, -1, false);
    }

    public Object searchByExample(FL_PatternDescriptor example, String service, long start, long max,
            int aptimaQueryIndex, boolean hmm) throws AvroRemoteException {
    	return searchByExample(example, service, start, max, aptimaQueryIndex, hmm, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public Object searchByExample(FL_PatternDescriptor example, String service, long start, long max,
            int aptimaQueryIndex, boolean hmm, long startTime, long endTime) throws AvroRemoteException {
        // TODO : support dateRange

        // returns FL_Future or FL_PatternSearchResults

        // inspected IDs in example and pick which binding to run against
        Binding binding = getBinding(example);

        if (binding != null) {
            logger.debug("search : " + example + " : " + aptimaQueryIndex + " hmm " + hmm + " binding " + binding);
            return binding.searchByExample(example, service, start, max, aptimaQueryIndex, hmm, startTime, endTime);
        } else {
            logger.warn("no binding");
        }

        // return dummy result

        FL_Entity entity = new FL_Entity();
        entity.setUid("B1234");
        List<FL_EntityTag> tags = new ArrayList<FL_EntityTag>();
        tags.add(FL_EntityTag.ACCOUNT);
        entity.setTags(tags);
        List<FL_Property> properties = new ArrayList<FL_Property>();
        entity.setProperties(properties);

        FL_EntityMatchResult entityMatch = new FL_EntityMatchResult();
        entityMatch.setScore(1.0);
        entityMatch.setUid("dummy");
        entityMatch.setEntity(entity);
        List<FL_EntityMatchResult> entityMatches = new ArrayList<FL_EntityMatchResult>();
        entityMatches.add(entityMatch);

        FL_PatternSearchResult result = new FL_PatternSearchResult();
        result.setScore(1.0);
        result.setEntities(entityMatches);

        List<FL_PatternSearchResult> results = new ArrayList<FL_PatternSearchResult>();
        results.add(result);

        FL_PatternSearchResults queryResult = new FL_PatternSearchResults();
        queryResult.setResults(results);
        queryResult.setTotal((long) results.size());

        return queryResult;
    }

    /**
     * @see #getBinding(influent.idl.FL_PatternDescriptor)
     * @param example
     * @return
     */
    private boolean useKiva(FL_PatternDescriptor example) {
        List<String> ids = Binding.getExemplarIDs(example);
        return useKiva(ids);
    }

    private boolean useKiva(List<String> ids) {
        boolean useKiva = false;
        for (String id : ids) {
            if (id.startsWith("l") || id.startsWith("p")) {
                useKiva = true;
                break;
            }
        }
        return useKiva;
    }

    @Override
    public FL_PatternSearchResults getResults(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<FL_PatternDescriptor> getPatternTemplates() throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<FL_Service> getServices() throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * @param entityID
     *            internal database id
     * @return
     * @throws Exception
     */
    public FL_Entity retrieveEntity_UNUSED(int entityID) throws Exception {
        // retrieve guid, name, and type
        String sql = "";
        sql += "select guid, name, type from entity where id = ?";

        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setInt(1, entityID);
        ResultSet rs = statement.executeQuery();

        FL_Entity entity = new FL_Entity();
        entity.setProperties(new ArrayList<FL_Property>());
        entity.setTags(new ArrayList<FL_EntityTag>());

        if (rs.next()) {
            entity.setUid(rs.getString(1));

            // default entity attributes that are currently in the entity table (vs auxiliary attribute tables)
            entity.getProperties().add(createProperty("name", rs.getString(2), FL_PropertyType.STRING));
            entity.getProperties().add(createProperty("type", rs.getString(3), FL_PropertyType.STRING));
        }
        rs.close();

        // add auxiliary attributes
        entity.getProperties().addAll(retrieveEntityAttributes(entityID, "string", FL_PropertyType.STRING));
        entity.getProperties().addAll(retrieveEntityAttributes(entityID, "int", FL_PropertyType.LONG));
        entity.getProperties().addAll(retrieveEntityAttributes(entityID, "double", FL_PropertyType.DOUBLE));
        entity.getProperties().addAll(retrieveEntityAttributes(entityID, "boolean", FL_PropertyType.BOOLEAN));
        entity.getProperties().addAll(retrieveEntityAttributes(entityID, "timestamp", FL_PropertyType.DATE));

        return entity;
    }

    /**
     * @param entityID
     *            internal database id
     * @param attributeType
     *            this corresponds to entity_attribute_? tables in database schema
     * @param propertyType
     * @return
     * @throws Exception
     */
    public List<FL_Property> retrieveEntityAttributes(int entityID, String attributeType, FL_PropertyType propertyType)
            throws Exception {
        List<FL_Property> properties = new ArrayList<FL_Property>();
        String sql = "select key, value from entity_attribute_" + attributeType + " where entity_id = ?";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setInt(1, entityID);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            String key = rs.getString(1);
            Object value = rs.getObject(2);
            properties.add(createProperty(key, value, propertyType));
        }
        rs.close();
        return properties;
    }

    public FL_Property createProperty(String key, Object value, FL_PropertyType type) {
        // TODO: add support for FL_PropertyTag
        FL_Property property = new FL_Property();
        property.setKey(key);
        property.setFriendlyText(key);
        if (type == null) {
            logger.warn("type is null");
        }
        logger.debug("value = " + value + ", type = " + type);
        property.setRange(new FL_SingletonRange(value, type));
        property.setTags(new ArrayList<FL_PropertyTag>());
        return property;
    }

    @Override
    public Object searchByTemplate(String template, String service, long start, long max, FL_BoundedRange dateRange)
            throws AvroRemoteException {
        // dummy result to return
        FL_Future future = new FL_Future();
        future.setUid("12345678");
        future.setLabel("searchByTemplate task");
        future.setService(null);
        future.setStarted(System.currentTimeMillis());
        future.setCompleted(-1L);
        return future;
    }
}
