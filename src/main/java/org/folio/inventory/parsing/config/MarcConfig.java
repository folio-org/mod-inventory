package org.folio.inventory.parsing.config;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.JsonHelper;
import org.folio.inventory.exceptions.InvalidMarcConfigException;

import java.io.IOException;

public class MarcConfig {

  private static final String MARC_FIELDS = "marc-fields";
  private static final String INSTANCE_FIELDS = "instance-fields";

  private final JsonObject config;
  private static final String STD_CONFIG_FILE = "config/marc-config.json";

  public MarcConfig() throws InvalidMarcConfigException {
    this(STD_CONFIG_FILE);
  }

  public MarcConfig(String configPath) throws InvalidMarcConfigException {
    try {
      config = JsonHelper.getJsonFileAsJsonObject(configPath);
    }
    catch (IOException e) {
      throw new InvalidMarcConfigException(configPath, e);
    }
    this.validate();
  }

  public JsonObject getConfig() {
    return config;
  }

  private void validate() throws InvalidMarcConfigException {
    if (!(config.getValue(MARC_FIELDS) instanceof JsonObject)) {
      throw new InvalidMarcConfigException("Key 'marc-fields' does not contain JsonObject..");
    }
    if (!(config.getValue(INSTANCE_FIELDS) instanceof JsonArray)) {
      throw new InvalidMarcConfigException("Key 'instance-fields' does not contain JsonArray..");
    }
    if (!(config.getJsonArray(INSTANCE_FIELDS).getValue(0) instanceof JsonObject)) {
      throw new InvalidMarcConfigException("JsonArray under key 'instance-fields' does not contain any JsonObject..");
    }
  }
}
