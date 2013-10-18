package mitll.xdata.graph;

import mitll.xdata.binding.KivaBinding;
import mitll.xdata.db.MysqlConnection;

import java.sql.Connection;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/8/13
 * Time: 7:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class FeatureEdge {
  private Connection connection;
  private KivaBinding kivaBinding = null;

  /**
   * so, get all the links
   * get all lenders
   * get all partners
   *
   * get features for lenders, partners
   * add features to edge between lender and partner
   * insert feature edge into kd tree
   *
   *
   * on search:
   *
   * find nearest in feature edge space
   *
   *
   *
   * find lender and partner entities for links
   *
   *
   *
   * convert into entities, and maybe links between them...
   *
   * for testing, we could use one feature, the name... and try to find nearest in name space...
   */
  public FeatureEdge() {
    try {
      MysqlConnection mysqlConnection = new MysqlConnection("kiva");
      connection = mysqlConnection.getConnection();
      kivaBinding = new KivaBinding(mysqlConnection);

      List<KivaBinding.LoanInfo> loanEdges = kivaBinding.getLoanEdges();

      //kivaBinding.getEntitiesByID()
    } catch (Exception e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
