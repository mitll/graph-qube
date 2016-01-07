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
