package mitll.xdata.dataset.bitcoin.ingest;

import mitll.xdata.db.DBConnection;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by go22670 on 8/6/15.
 */
public class IngestSql {
  private static final Logger logger = Logger.getLogger(BitcoinIngestRaw.class);

  private static final Map<String, String> TYPE_TO_DB = new HashMap<String, String>();

  static {
    TYPE_TO_DB.put("INTEGER", "INT");
    TYPE_TO_DB.put("STRING", "VARCHAR");
    TYPE_TO_DB.put("DATE", "TIMESTAMP");
    TYPE_TO_DB.put("REAL", "DOUBLE");
    TYPE_TO_DB.put("BOOLEAN", "BOOLEAN");
  }

  void createTable(String tableName, List<String> cnames, List<String> types, DBConnection connection) throws SQLException {
    long t = System.currentTimeMillis();
    logger.debug("dropping current " + tableName);
    doSQL(connection, "DROP TABLE " + tableName + " IF EXISTS");
    logger.debug("took " + (System.currentTimeMillis() - t) + " millis to drop " + tableName);
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
    logger.debug("first index complete in " + (System.currentTimeMillis() - then));
    then = System.currentTimeMillis();
    doSQL(connection, "CREATE INDEX ON " + tableName + " (" + "TARGET" + ")");
    logger.debug("second index complete in " + (System.currentTimeMillis() - then));

    then = System.currentTimeMillis();
    doSQL(connection, "CREATE INDEX ON " + tableName + " (" + "TIME" + ")");
    logger.debug("third index complete in " + (System.currentTimeMillis() - then));

    doSQL(connection, "create index " +
        //"idx_transactions_source_target" +
        " on " +
        tableName +
        "(" +
        "SOURCE" + ", TARGET" + ")");
    logger.debug("fourth index complete in " + (System.currentTimeMillis() - then));
  }
}
