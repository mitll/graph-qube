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

package mitll.xdata.binding;

import influent.idl.FL_PatternSearchResult;
import mitll.xdata.GraphQuBEServer;
import mitll.xdata.ServerProperties;
import mitll.xdata.SimplePatternSearch;
import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Created by go22670 on 1/8/16.
 */
@RunWith(JUnit4.class)
public class TopKTest {
  private static Logger logger = Logger.getLogger(TopKTest.class);


  @Test
  public void testSearch() {
    logger.debug("ENTER testSearch()");
    ServerProperties props = new ServerProperties();

    String kivaDirectory = ".";
    String bitcoinDirectory = ".";
    String bitcoinFeatureDirectory = GraphQuBEServer.DEFAULT_BITCOIN_FEATURE_DIR;

    try {
      DBConnection dbConnection = props.useMysql() ? new MysqlConnection(props.mysqlBitcoinJDBC()) : new H2Connection(bitcoinDirectory,"bitcoin");
      final SimplePatternSearch patternSearch;
      patternSearch = new SimplePatternSearch();

      BitcoinBinding bitcoinBinding = new BitcoinBinding(dbConnection, bitcoinFeatureDirectory);
      patternSearch.setBitcoinBinding(bitcoinBinding);
      Shortlist shortlist = bitcoinBinding.getShortlist();
      List<FL_PatternSearchResult> shortlist1 = shortlist.getShortlist(null, Arrays.asList("555261", "400046", "689982", "251593"), 10);

      if (shortlist1.size() > 10) {
        shortlist1 = shortlist1.subList(0,10);
      }
      for (FL_PatternSearchResult result:shortlist1) logger.info("got " + result);

    } catch (Exception e) {
      logger.error("got " + e,e);

    }

//    Assert.assertEquals(sequence.getStates(), makeStates(1, 1, 1, 2, 2, 2));

    logger.debug("EXIT testSearch()");
  }
}

