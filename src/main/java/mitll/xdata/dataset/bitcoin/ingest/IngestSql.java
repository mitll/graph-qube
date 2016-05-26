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

package mitll.xdata.dataset.bitcoin.ingest;

import mitll.xdata.dataset.bitcoin.features.MysqlInfo;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by go22670 on 8/6/15.
 */
public class IngestSql {
  private static final Logger logger = Logger.getLogger(IngestSql.class);
  private static final Map<String, String> TYPE_TO_DB = new HashMap<>();
  // public static final String SOURCE1 = "SOURCE";
  public static final String SOURCE = "SOURCE";
  static final String TARGET = "TARGET";
  static final String TIME = "TIME";
  static final String ID_COL_TYPE = "BIGINT";

  static {
    TYPE_TO_DB.put("INTEGER", "INT");
    TYPE_TO_DB.put("STRING", "VARCHAR");
    TYPE_TO_DB.put("DATE", "TIMESTAMP");
    TYPE_TO_DB.put("REAL", "DOUBLE");
    TYPE_TO_DB.put("BOOLEAN", "BOOLEAN");
  }

  DBConnection getDbConnection(String dbType, String h2DatabaseName) throws Exception {
    if (dbType.equals("mysql")) logger.error("not expecting " + dbType + " " + h2DatabaseName);

    return dbType.equalsIgnoreCase("h2") ?
        new H2Connection(h2DatabaseName, 10000000, true) : dbType.equalsIgnoreCase("mysql") ?
        new MysqlConnection(h2DatabaseName) : null;
  }

  /**
   * @param dbType
   * @param tableName
   * @param useTimestamp
   * @param connection
   * @throws SQLException
   * @see BitcoinIngestUnchartedTransactions#loadTransactionTable(MysqlInfo, String, String, String, boolean, int)
   */
  void createTable(String dbType, String tableName, boolean useTimestamp, DBConnection connection) throws SQLException {
    List<String> cnames = getColumnsForTransactionsTable();

    List<String> types = Arrays.asList(ID_COL_TYPE, ID_COL_TYPE, ID_COL_TYPE,
        useTimestamp ? "TIMESTAMP" : "LONG", "DECIMAL(20, 8)",
        "DECIMAL(20, 8)", "DECIMAL", "DECIMAL", "DECIMAL", "ARRAY"); // bitcoin seems to allow 8 digits after the decimal

    if (dbType.equals("h2")) tableName = tableName.toUpperCase();

    createTable(connection, tableName, cnames, types);

    //doSQL(connection, "ALTER TABLE " + tableName + " ALTER COLUMN UID INT NOT NULL");
    //doSQL(connection, "ALTER TABLE " + tableName + " ADD PRIMARY KEY (UID)");
  }

  /**
   * @return
   * @see
   */
  List<String> getColumnsForTransactionsTable() {
    List<String> cols = new ArrayList<>(getColumnsForInsert());
    cols.add(BitcoinIngestSubGraph.SORTED_PAIR);
    return cols;
    //return Arrays.asList("TRANSID", "SOURCE", "TARGET", "TIME", "AMOUNT", "USD", "DEVPOP", "CREDITDEV", "DEBITDEV", BitcoinIngestSubGraph.SORTED_PAIR);
  }

  List<String> getColumnsForInsert() {
    return Arrays.asList("TRANSID", SOURCE, TARGET, TIME, "AMOUNT", "USD", "DEVPOP", "CREDITDEV", "DEBITDEV");
  }

  /**
   * @param connection
   * @param tableName
   * @param cnames
   * @param types
   * @throws SQLException
   * @see #createTable(String, String, boolean, DBConnection)
   */
  private void createTable(DBConnection connection, String tableName, List<String> cnames, List<String> types) throws SQLException {
    long t = System.currentTimeMillis();
    //  logger.debug("dropping current " + tableName);
    doSQL(connection, "DROP TABLE " + tableName + " IF EXISTS");
    //  logger.debug("took " + (since(t)) + " millis to drop " + tableName);
    doSQL(connection, createCreateSQL(tableName, cnames, types, false));
  }

  /**
   * @param tableName
   * @param names
   * @param types
   * @param mapTypes
   * @return
   * @see #createTable(DBConnection, String, List, List)
   */
  private String createCreateSQL(String tableName, List<String> names, List<String> types, boolean mapTypes) {
    String sql = "CREATE TABLE " + tableName + " (" + "\n";
    for (int i = 0; i < names.size(); i++) {
      String statedType = types.get(i).toUpperCase();
      if (mapTypes) statedType = TYPE_TO_DB.get(statedType);
      if (statedType == null) logger.error("huh? unknown type " + types.get(i));
      sql += (i > 0 ? ",\n  " : "  ") + names.get(i) + " " + statedType;
    }
    sql += "\n);";

//    logger.debug("create " + sql);
    return sql;
  }

  String createInsertSQL(String tableName, List<String> names) {
    String sql = "INSERT INTO " + tableName + " (";
    for (int i = 0; i < names.size(); i++) {
      sql += (i > 0 ? ", " : "") + names.get(i);
    }
    sql += ") VALUES (";
    for (int i = 0; i < names.size(); i++) {
      sql += (i > 0 ? ", " : "") + "?";
    }
    sql += ")";
    return sql;
  }

  private void doSQL(DBConnection connection, String createSQL) throws SQLException {
    Connection connection1 = connection.getConnection();
    PreparedStatement statement = connection1.prepareStatement(createSQL);
    statement.executeUpdate();
    statement.close();
  }

  /**
   * @param tableName
   * @param connection
   * @throws SQLException
   * @see BitcoinIngestSubGraph#extractUndirectedGraphInMemory(DBConnection, Collection)
   */
  void createIndices(String tableName, DBConnection connection) throws SQLException {
    long then = System.currentTimeMillis();
    long start = then;
    doSQL(connection, "CREATE INDEX ON " + tableName + " (" + SOURCE + ")");
    logger.debug("first (" + SOURCE + ") index complete in " + reportTime(then) + " on " + tableName);
    then = System.currentTimeMillis();
    doSQL(connection, "CREATE INDEX ON " + tableName + " (" + TARGET + ")");
    logger.debug("second (" + TARGET +
        ") index complete in " + (reportTime(then)) + " on " + tableName);

    then = System.currentTimeMillis();
    doSQL(connection, "CREATE INDEX ON " + tableName + " (" + TIME + ")");
    logger.debug("third (" + TIME +
        ") index complete in " + (reportTime(then)) + " on " + tableName);

    then = System.currentTimeMillis();
    doSQL(connection, "create index " +
        //"idx_transactions_source_target" +
        " on " +
        tableName +
        "(" +
        SOURCE + ", " +
        TARGET + ")");
    logger.debug("fourth (" + SOURCE + ", " + TARGET + ")" +
        " index complete in " + reportTime(then) + " on " + tableName);
    logger.debug("overall took " + reportTime(start) + " to do indices");
  }

  private String reportTime(long then) {
    return (since(then)) + " seconds ";
  }

  private long since(long then) {
    return (System.currentTimeMillis() - then) / 1000;
  }
}
