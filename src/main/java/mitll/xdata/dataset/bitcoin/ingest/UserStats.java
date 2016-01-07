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

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/25/13
 * Time: 2:26 PM
 * To change this template use File | Settings | File Templates.
 *
 * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngest#loadTransactionTable(String, String, String, String, boolean)
 * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngest#addFeatures(String, java.util.Map, double, mitll.xdata.dataset.bitcoin.ingest.BitcoinIngest.RateConverter)
 */
public class UserStats {
  double cnum, dnum;
  double creditTotal;
  double debitTotal;
  public void addCredit(double c) { creditTotal += c; cnum++; }
  public void addDebit(double c)  { debitTotal += c;  dnum++; }
  public double getAvgCredit() { return cnum == 0 ? 0 : creditTotal/cnum; }
  public double getAvgDebit()  { return dnum == 0 ? 0 : debitTotal/dnum; }
}
