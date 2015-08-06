package mitll.xdata.dataset.bitcoin.ingest;

import java.sql.Timestamp;

/**
 * Created by go22670 on 8/6/15.
 */
public class BitcoinTransaction {
  int sourceid;
  int targetID;

 // Timestamp timestamp;
  long timeMillis;
  double btc;
  double dollar;

  public BitcoinTransaction(String line) {

  }
  public boolean parse(String line) {
    String[] split = line.split("\\s+"); // 4534248 25      25      2013-01-27 22:41:38     9.91897304
    if (split.length != 6) {
      return false;
    }

     sourceid = Integer.parseInt(split[1]);
     targetID = Integer.parseInt(split[2]);

    String day = split[3];
    Timestamp x = Timestamp.valueOf(day + " " + split[4]);
    timeMillis = x.getTime();
    return true;
  }
}
