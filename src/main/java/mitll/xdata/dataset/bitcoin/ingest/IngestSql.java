package mitll.xdata.dataset.bitcoin.ingest;

import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by go22670 on 8/6/15.
 */
public class IngestSql {
  private static final Logger logger = Logger.getLogger(IngestSql.class);

  private static final Map<String, String> TYPE_TO_DB = new HashMap<String, String>();

  static {
    TYPE_TO_DB.put("INTEGER", "INT");
    TYPE_TO_DB.put("STRING", "VARCHAR");
    TYPE_TO_DB.put("DATE", "TIMESTAMP");
    TYPE_TO_DB.put("REAL", "DOUBLE");
    TYPE_TO_DB.put("BOOLEAN", "BOOLEAN");
  }

  DBConnection getDbConnection(String dbType, String h2DatabaseName) throws Exception {
    return dbType.equalsIgnoreCase("h2") ?
        new H2Connection(h2DatabaseName, 10000000, true) : dbType.equalsIgnoreCase("mysql") ?
        new MysqlConnection(h2DatabaseName) : null;
  }

  void createTable(String dbType, String tableName, boolean useTimestamp, DBConnection connection) throws SQLException {
    List<String> cnames = getColumnsForTransactionsTable();

    List<String> types = Arrays.asList("INT", "INT", "INT", useTimestamp ? "TIMESTAMP" : "LONG", "DECIMAL(20, 8)",
        "DECIMAL(20, 8)", "DECIMAL", "DECIMAL", "DECIMAL"); // bitcoin seems to allow 8 digits after the decimal

    if (dbType.equals("h2")) tableName = tableName.toUpperCase();

    createTable(connection, tableName, cnames, types);

    //doSQL(connection, "ALTER TABLE " + tableName + " ALTER COLUMN UID INT NOT NULL");
    //doSQL(connection, "ALTER TABLE " + tableName + " ADD PRIMARY KEY (UID)");
  }

  List<String> getColumnsForTransactionsTable() {
    return Arrays.asList("TRANSID", "SOURCE", "TARGET", "TIME", "AMOUNT", "USD", "DEVPOP", "CREDITDEV", "DEBITDEV");
  }

  void createTable(DBConnection connection, String tableName, List<String> cnames, List<String> types) throws SQLException {
    long t = System.currentTimeMillis();
    logger.debug("dropping current " + tableName);
    doSQL(connection, "DROP TABLE " + tableName + " IF EXISTS");
    logger.debug("took " + (since(t)) + " millis to drop " + tableName);
    doSQL(connection, createCreateSQL(tableName, cnames, types, false));
  }

  private  String createCreateSQL(String tableName, List<String> names, List<String> types, boolean mapTypes) {
    String sql = "CREATE TABLE " + tableName + " (" + "\n";
    for (int i = 0; i < names.size(); i++) {
      String statedType = types.get(i).toUpperCase();
      if (mapTypes) statedType = TYPE_TO_DB.get(statedType);
      if (statedType == null) logger.error("huh? unknown type " + types.get(i));
      sql += (i > 0 ? ",\n  " : "  ") + names.get(i) + " " + statedType;
    }
    sql += "\n);";

 //   logger.debug("create " + sql);
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
    sql += ");";
    return sql;
  }

  void doSQL(DBConnection connection, String createSQL) throws SQLException {
    Connection connection1 = connection.getConnection();
    PreparedStatement statement = connection1.prepareStatement(createSQL);
    statement.executeUpdate();
    statement.close();
  }

  void createIndices(String tableName, DBConnection connection) throws SQLException {
    long then = System.currentTimeMillis();
    doSQL(connection, "CREATE INDEX ON " + tableName + " (" + "SOURCE" + ")");
    logger.debug("first  index complete in " + since(then) + " on " + tableName);
    then = System.currentTimeMillis();
    doSQL(connection, "CREATE INDEX ON " + tableName + " (" + "TARGET" + ")");
    logger.debug("second index complete in " + (since(then)) + " on " + tableName);

    then = System.currentTimeMillis();
    doSQL(connection, "CREATE INDEX ON " + tableName + " (" + "TIME" + ")");
    logger.debug("third  index complete in " + (since(then)) + " on " + tableName);

    doSQL(connection, "create index " +
        //"idx_transactions_source_target" +
        " on " +
        tableName +
        "(" +
        "SOURCE" + ", TARGET" + ")");
    logger.debug("fourth index complete in " + (since(then)) + " on " + tableName);
  }

  private long since(long then) {
    return System.currentTimeMillis() - then;
  }
}
