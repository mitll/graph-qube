// Copyright 2013 MIT Lincoln Laboratory, Massachusetts Institute of Technology 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mitll.xdata;

import influent.idl.*;
import mitll.xdata.binding.Binding;
import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import org.apache.avro.AvroRemoteException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class SimplePatternSearch implements FL_PatternSearch {
  private static Logger logger = Logger.getLogger(SimplePatternSearch.class);

  private Binding kivaBinding = null;
  private Binding bitcoinBinding = null;

  public SimplePatternSearch() {}

    @Override
    public Void setTimeout(FL_Future future, long timeout) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean getCompleted(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getError(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public double getProgress(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getExpectedDuration(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Void stop(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<FL_Future> getFutures() throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    public Binding getBinding(FL_PatternDescriptor example) {
        boolean useKiva = useKiva(example);

        logger.debug("for " + example + " use kiva : " + useKiva);
        if (useKiva && kivaBinding != null) {
            return kivaBinding;
        } else if (bitcoinBinding != null) {
            return bitcoinBinding;
        }
        return null;
    }

    @Override
    public Object searchByExample(FL_PatternDescriptor example, String service, long start, long max,
            FL_BoundedRange dateRange) throws AvroRemoteException {
        // TODO : support dateRange
        return searchByExample(example, service, start, max, false);
    }

    public Object searchByExample(FL_PatternDescriptor example, String service, long start, long max,
                                  boolean hmm) throws AvroRemoteException {
    	return searchByExample(example, service, start, max, hmm, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public Object searchByExample(FL_PatternDescriptor example, String service, long start, long max,
                                  boolean hmm, long startTime, long endTime) throws AvroRemoteException {
        // TODO : support dateRange

        // returns FL_Future or FL_PatternSearchResults

        // inspected IDs in example and pick which binding to run against
        Binding binding = getBinding(example);

        if (binding != null) {
            logger.debug("search : " + example + " hmm " + hmm + " binding " + binding);
            return binding.searchByExample(example, service, start, max, hmm, startTime, endTime);
        } else {
            logger.error("no binding");
        }

        // return dummy result - maybe this is better than nothing?
        return makeNoBindingResponse();
    }

  private FL_PatternSearchResults makeNoBindingResponse() {
    FL_Entity entity = new FL_Entity();
    entity.setUid("B1234");
    List<FL_EntityTag> tags = new ArrayList<FL_EntityTag>();
    tags.add(FL_EntityTag.ACCOUNT);
    entity.setTags(tags);
    List<FL_Property> properties = new ArrayList<FL_Property>();
    entity.setProperties(properties);

    FL_EntityMatchResult entityMatch = new FL_EntityMatchResult();
    entityMatch.setScore(1.0);
    entityMatch.setUid("dummy");
    entityMatch.setEntity(entity);
    List<FL_EntityMatchResult> entityMatches = new ArrayList<FL_EntityMatchResult>();
    entityMatches.add(entityMatch);

    FL_PatternSearchResult result = new FL_PatternSearchResult();
    result.setScore(1.0);
    result.setEntities(entityMatches);

    List<FL_PatternSearchResult> results = new ArrayList<FL_PatternSearchResult>();
    results.add(result);

    FL_PatternSearchResults queryResult = new FL_PatternSearchResults();
    queryResult.setResults(results);
    queryResult.setTotal((long) results.size());
    return queryResult;
  }

  /**
     * @see #getBinding(influent.idl.FL_PatternDescriptor)
     * @param example
     * @return
     */
    private boolean useKiva(FL_PatternDescriptor example) {
        List<String> ids = Binding.getExemplarIDs(example);
        return useKiva(ids);
    }

    private boolean useKiva(List<String> ids) {
        boolean useKiva = false;
        for (String id : ids) {
            if (id.startsWith("l") || id.startsWith("p")) {
                useKiva = true;
                break;
            }
        }
        return useKiva;
    }

    @Override
    public FL_PatternSearchResults getResults(FL_Future future) throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<FL_PatternDescriptor> getPatternTemplates() throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<FL_Service> getServices() throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object searchByTemplate(String template, String service, long start, long max, FL_BoundedRange dateRange)
            throws AvroRemoteException {
        // dummy result to return
        FL_Future future = new FL_Future();
        future.setUid("12345678");
        future.setLabel("searchByTemplate task");
        future.setService(null);
        future.setStarted(System.currentTimeMillis());
        future.setCompleted(-1L);
        return future;
    }

  public void setKivaBinding(Binding kivaBinding) {
    this.kivaBinding = kivaBinding;
  }

  public void setBitcoinBinding(Binding bitcoinBinding) {
    this.bitcoinBinding = bitcoinBinding;
  }
}
