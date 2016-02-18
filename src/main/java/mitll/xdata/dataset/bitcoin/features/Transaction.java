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

/**
 * Created by go22670 on 2/17/16.
 */ //@SuppressWarnings("CanBeFinal")
public class Transaction implements Comparable<Transaction> {
  private final int source;
  private final int target;
  private final long time;
  private final double amount;
/*    public Transaction(ResultSet rs) throws Exception {
    int i = 1;
    source = rs.getInt(i++);
    target = rs.getInt(i++);
    //time = rs.getTimestamp(i++).getTime();
    time = rs.getLong(i++);
    amount = rs.getDouble(i);
  }*/

  public Transaction(int source, int target, long time, double amount) {
    this.source = source;
    this.target = target;
    this.time = time;
    this.amount = amount;
  }

  @Override
  public int compareTo(Transaction o) {
    return
        time < o.time ? -1 : time > o.time ? +1 :
            amount < o.amount ? -1 : amount > o.amount ? +1 :
                target < o.target ? -1 : target > o.target ? +1 : 0;
  }

  public int getSource() {
    return source;
  }

  public int getTarget() {
    return target;
  }

  public long getTime() {
    return time;
  }

  public double getAmount() {
    return amount;
  }
}
