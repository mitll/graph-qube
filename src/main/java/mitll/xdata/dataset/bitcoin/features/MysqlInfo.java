package mitll.xdata.dataset.bitcoin.features;

import mitll.xdata.db.MysqlConnection;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by go22670 on 8/7/15.
 */
public class MysqlInfo {
  public static final String TRANSACTION_ID = "TransactionId";
  public static final String SENDER_ID = "SenderId";
  public static final String RECEIVER_ID = "ReceiverId";
  public static final String TX_TIME = "TxTime";
  public static final String BTC = "BTC";
  public static final String USD = "USD";

  private String jdbc;
  private String table;
  private Map<String, String> slotToCol = new HashMap<>();

  public MysqlInfo() {
    this(new MysqlConnection().getSimpleURL("bitcoin"), "usertransactions2013largerthandollar", null);

    slotToCol = new HashMap<>();
    for (String col : Arrays.asList(TRANSACTION_ID, SENDER_ID, RECEIVER_ID, TX_TIME, BTC, USD)) {
      getSlotToCol().put(col, col);
    }
  }

  public MysqlInfo(String jdbc, String table, Map<String, String> slotToCol) {
    this.jdbc = jdbc;
    this.table = table;
    this.slotToCol = slotToCol;
  }

  public String getJdbc() {
    return jdbc;
  }

  public String getTable() {
    return table;
  }

  public Map<String, String> getSlotToCol() {
    return slotToCol;
  }
}
