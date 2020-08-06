package org.folio.inventory.kafka;


import java.util.Objects;

public class SimpleConfigurationReader {
  private SimpleConfigurationReader() {
    super();
  }

  public static String getValue(String key, String defValue) {
    String value = System.getenv(key);
    return (Objects.nonNull(value) && !value.isEmpty()) ? value : defValue;
  }
}
