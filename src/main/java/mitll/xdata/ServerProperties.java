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

  private Properties props = new Properties();

  public boolean useMysql() { return getDefaultFalse("useMysql"); }
  public boolean useKiva () { return getDefaultFalse("useKiva"); }

  public ServerProperties() {
    readProps();
  }

  /**
   * @paramx configDir
   * @paramx configFile
   * @paramx dateFromManifest
   */
  private void readProps() {
    String configFileFullPath = "server.properties";
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

  private void readProperties(String dateFromManifest) {
    if (dateFromManifest != null && dateFromManifest.length() > 0) {
      props.setProperty("releaseDate", dateFromManifest);
    }
  }

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
