package mitll.xdata;

import influent.idl.*;
import mitll.xdata.binding.Binding;
import mitll.xdata.binding.TestBinding;
import org.apache.avro.AvroRemoteException;

import java.util.List;

/**
 * Created by go22670 on 4/7/14.
 */
public class TestPatternSearch implements FL_PatternSearch {
  //private TestBinding testBinding = null;

  private TestBinding testBinding = null;

  /**
   * Creates SimplePatternSearch that can detect Kiva vs Bitcoin ids in query.
   *
   * Assumes that kiva.h2.db and bitcoin.h2.db are live in kivaDirectory and bitcoinDirectory.
   *
   * @see GraphQuBEServer#main(String[])
   */
  public static TestPatternSearch getDemoPatternSearch(String kivaDirectory, String bitcoinDirectory,
                                                         boolean useFastBitcoinConnectedTest) throws Exception {
    TestPatternSearch search = new TestPatternSearch();

    search.testBinding = new TestBinding();

    return search;
  }

  public Binding getTestBinding() { return testBinding; }

  @Override
  public Void setTimeout(FL_Future future, long timeout) throws AvroRemoteException {
    return null;
  }

  @Override
  public boolean getCompleted(FL_Future future) throws AvroRemoteException {
    return false;
  }

  @Override
  public String getError(FL_Future future) throws AvroRemoteException {
    return null;
  }

  @Override
  public double getProgress(FL_Future future) throws AvroRemoteException {
    return 0;
  }

  @Override
  public long getExpectedDuration(FL_Future future) throws AvroRemoteException {
    return 0;
  }

  @Override
  public Void stop(FL_Future future) throws AvroRemoteException {
    return null;
  }

  @Override
  public List<FL_Future> getFutures() throws AvroRemoteException {
    return null;
  }

  @Override
  public Object searchByExample(FL_PatternDescriptor example, String service, long start, long max, FL_BoundedRange dateRange) throws AvroRemoteException {
    return null;
  }

  @Override
  public Object searchByTemplate(String template, String service, long start, long max, FL_BoundedRange dateRange) throws AvroRemoteException {
    return null;
  }

  @Override
  public FL_PatternSearchResults getResults(FL_Future future) throws AvroRemoteException {
    return null;
  }

  @Override
  public List<FL_PatternDescriptor> getPatternTemplates() throws AvroRemoteException {
    return null;
  }

  @Override
  public List<FL_Service> getServices() throws AvroRemoteException {
    return null;
  }
}
