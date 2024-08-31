package org.folio.inventory.config;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PropertiesReader {

  private static final String PROPERTIES_FILE = "/application.yaml";

  protected static final Logger LOGGER = LogManager.getLogger(
    PropertiesReader.class);
  private static final Properties properties = new Properties();

  static {
    loadProperties();
  }

  private PropertiesReader() {
  }

  private static void loadProperties() {
    try (InputStream inputStream = PropertiesReader.class.getResourceAsStream(
      PROPERTIES_FILE)) {
      properties.load(inputStream);
    } catch (IOException e) {
      LOGGER.error("Couldn't load application configuration file", e);
    }
  }

  public static boolean getPropertyAsBoolean(String key) {
    String value = properties.getProperty(key);
    return Boolean.parseBoolean(value);
  }

  public static void setProperty(String key, String value) {
    properties.setProperty(key, value);
  }

  public static void clearProperties() {
    properties.clear();
    loadProperties();
  }
}
