package mitll.xdata.ingest;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/25/13
 * Time: 2:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserStats {
  double cnum, dnum;
  double creditTotal;
  double debitTotal;
  void addCredit(double c) { creditTotal += c; cnum++; }
  void addDebit(double c)  { debitTotal += c;  dnum++; }
  public double getAvgCredit() { return cnum == 0 ? 0 : creditTotal/cnum; }
  public double getAvgDebit()  { return dnum == 0 ? 0 : debitTotal/dnum; }
}
