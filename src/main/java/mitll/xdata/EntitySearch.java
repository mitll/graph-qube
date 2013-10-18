package mitll.xdata;

import influent.idl.FL_EntitySearch;
import influent.idl.FL_PropertyDescriptor;
import influent.idl.FL_PropertyMatchDescriptor;
import influent.idl.FL_SearchResults;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import mitll.xdata.binding.KivaBinding;
import mitll.xdata.db.MysqlConnection;

import org.apache.avro.AvroRemoteException;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/8/13
 * Time: 5:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class EntitySearch implements FL_EntitySearch {
  private Connection connection;
  private KivaBinding kivaBinding = null;

  public EntitySearch() throws Exception {
    MysqlConnection mysqlConnection = new MysqlConnection("kiva");
    connection = mysqlConnection.getConnection();
    kivaBinding = new KivaBinding(mysqlConnection);
  }
  public EntitySearch(String username, String password) throws Exception {
    MysqlConnection mysqlConnection = new MysqlConnection("kiva",username, password);
    if (mysqlConnection.isValid())
      connection = mysqlConnection.getConnection();
    kivaBinding = new KivaBinding(mysqlConnection);
/*
    if (kivaBinding != null) {
      return kivaBinding.searchByExample(example, service, start, max);
    }*/
  }

  @Override
  public Map<String, List<FL_PropertyDescriptor>> getDescriptors() throws AvroRemoteException {
    return Collections.emptyMap();
  }
  
    @Override
    public FL_SearchResults search(String query, List<FL_PropertyMatchDescriptor> terms, long start, long max,
            String type) throws AvroRemoteException {
        return kivaBinding.simpleSearch(terms, start, max);
    }
}
