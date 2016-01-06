package mitll.xdata.dataset.bitcoin.features;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by go22670 on 8/7/15.
 */
public class FeaturesSql {
  private static final Logger logger = Logger.getLogger(FeaturesSql.class);
  public static final String USERS = "USERS";

  public void createUsersTable(Connection connection) throws SQLException {
    logger.info("FeaturesSql create users table");

  /*
   * Make USERS table
   */
    String createTable = getCreateSQL();
    String sqlMakeTable = "drop table " +USERS+  " if exists;" +  createTable;
    PreparedStatement statement = connection.prepareStatement(sqlMakeTable);
    statement.executeUpdate();
    statement.close();
  }

  private String getCreateSQL() {
    return " create table IF NOT EXISTS " +
        USERS +
        " (USER integer(10)," +
        " CREDIT_MEAN double," +
        " CREDIT_STD double," +
        " CREDIT_INTERARR_MEAN double," +
        " CREDIT_INTERARR_STD double," +
        " DEBIT_MEAN double," +
        " DEBIT_STD double," +
        " DEBIT_INTERARR_MEAN double," +
        " DEBIT_INTERARR_STD double," +
        " PERP_IN double," +
        " PERP_OUT double," +
        " TYPE int not null default(1)" +
        ")";
  }

  public void createUsersTableNoDrop(Connection connection) throws SQLException {
    logger.info("create users table");

  /*
   * Make USERS table
   */
    String createTable = getCreateSQL();
    PreparedStatement statement = connection.prepareStatement(createTable);
    statement.executeUpdate();
    statement.close();
  }
}
