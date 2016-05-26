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

package mitll.xdata;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by go22670 on 7/31/15.
 */
public class ServerProperties {
  private static final Logger logger = Logger.getLogger(ServerProperties.class);

//  private static final String ENTITYID = "entityid";
  //private static final String FINENTITY = "FinEntity";

  private Properties props = new Properties();

  public boolean useMysql() {
    return getDefaultFalse("useMysql");
  }

  boolean useKiva() {
    return getDefaultFalse("useKiva");
  }

  private static String getPropsFile(String[] args) {
    String propsFile = null;
    for (String arg : args) {


      String prefix = "props=";
      logger.info("got " + arg);

      if (arg.startsWith(prefix)) {
        propsFile = getValue(arg, prefix);
      }
    }
    return propsFile;
  }

  private static String getValue(String arg, String prefix) {
    return arg.split(prefix)[1];
  }

  public ServerProperties(String [] args) {
    readProps(getPropsFile(args));
  }

  /**
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestUncharted#main(String[])
   * @param props
   */
  public ServerProperties(String props) {
    readProps(props);
  }

  public String getSourceDatabase() {
    return props.getProperty("database");
  }

  public String getFeatureDatabase() {
    return props.getProperty("featuresDatabase");
  }

  /**
   * @paramx configDir
   * @paramx configFile
   * @paramx dateFromManifest
   */
  private void readProps() {
    String configFileFullPath = "server.properties";
    readProps(configFileFullPath);
  }

  private void readProps(String configFileFullPath) {
    if (!new File(configFileFullPath).exists()) {
      logger.error("couldn't find config file " + new File(configFileFullPath).getAbsolutePath());
    } else {
      try {
        props = new Properties();
        props.load(new FileInputStream(configFileFullPath));
        //    readProperties(dateFromManifest);
      } catch (IOException e) {
        logger.error("got " + e, e);
      }
    }
  }

/*  private void readProperties(String dateFromManifest) {
    if (dateFromManifest != null && dateFromManifest.length() > 0) {
      props.setProperty("releaseDate", dateFromManifest);
    }
  }*/

  private boolean getDefaultFalse(String param) {
    return props.getProperty(param, "false").equals("true");
  }

  private boolean getDefaultTrue(String param) {
    return props.getProperty(param, "true").equals("true");
  }

  public String mysqlKivaJDBC() {
    return props.getProperty("mysqlKivaJDBC", "");
  }

  public String h2KivaJDBC() {
    return props.getProperty("h2KivaJDBC", "");
  }

  public String mysqlBitcoinJDBC() {
    return props.getProperty("mysqlBitcoinJDBC", "");
  }

  public String h2BitcoinJDBC() {
    return props.getProperty("h2BitcoinJDBC", "");
  }

  public String getEntityID() {
    return props.getProperty("entityID");
  }

  public String getNumTransactions() {
    return props.getProperty("numtransactions", "numtransactions");
  }

  public String getTransactionsTable() {
    return props.getProperty("transactionsTable", "usertransactions2013largerthandollar");
  }

  public String getFinEntity() {
    return props.getProperty("finEntity");
  }

  public String getTransactionsID() {
    return props.getProperty("transactionsID");
  }

  public String getSenderID() {
    return props.getProperty("transactionsSender");
  }

  public String getReceiverID() {
    return props.getProperty("transactionsReceiver");
  }

  public String getTransactionTime() {
    return props.getProperty("transactionsTxTime");
  }

  public String getBitcoinAmount() {
    return props.getProperty("transactionsBTC");
  }

  public String getUSDAmount() {
    return props.getProperty("transactionsUSD");
  }

  public String getDatasetID() {
    return props.getProperty("dataset");
  }

  public String getDatasetResourceDir() {
    return props.getProperty("datasetResourceDir");
  }

//  private String getDateFromManifest(ServletContext servletContext) {
//    try {
//      InputStream inputStream = servletContext.getResourceAsStream("/META-INF/MANIFEST.MF");
//      Manifest manifest = new Manifest(inputStream);
//      Attributes attributes = manifest.getMainAttributes();
//      return attributes.getValue("Built-Date");
//    } catch (Exception ex) {
//    }
//    return "";
//  }
}
