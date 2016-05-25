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

package mitll.xdata.dataset.bitcoin.features;

import mitll.xdata.ServerProperties;
import mitll.xdata.db.MysqlConnection;

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
  // public static final String BITCOIN = "bitcoin";
  // public static final String USERTRANSACTIONS_2013_LARGERTHANDOLLAR = "usertransactions2013largerthandollar";

  private String jdbc;
  private String table;
  private Map<String, String> slotToCol = new HashMap<>();

  /**
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestUncharted#doIngest
   */
  public MysqlInfo(ServerProperties props) {
    this(new MysqlConnection().getSimpleURL(props.getSourceDatabase()), props.getTransactionsTable());
    slotToCol = new HashMap<>();

    slotToCol.put(TRANSACTION_ID, props.getTransactionsID());
    slotToCol.put(SENDER_ID, props.getSenderID());
    slotToCol.put(RECEIVER_ID, props.getReceiverID());
    slotToCol.put(TX_TIME, props.getTransactionTime());
    slotToCol.put(BTC, props.getBitcoinAmount());
    slotToCol.put(USD, props.getUSDAmount());
  }

  private MysqlInfo(String jdbc, String table) {
    this.setJdbc(jdbc);
    this.setTable(table);
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

  public void setJdbc(String jdbc) {
    this.jdbc = jdbc;
  }

  public void setTable(String table) {
    this.table = table;
  }
}
