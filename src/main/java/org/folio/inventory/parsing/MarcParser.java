package org.folio.inventory.parsing;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import org.folio.inventory.exceptions.InvalidMarcJsonException;
import org.folio.inventory.parsing.config.MarcConfig;
import org.folio.inventory.exceptions.InvalidMarcConfigException;
import org.folio.inventory.support.JsonHelper;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

public class MarcParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String FIELDS = "fields";
  private static final String SUBFIELDS = "subfields";
  private static final String MARC_FIELDS = "marc-fields";
  private static final String INSTANCE_FIELDS = "instance-fields";
  private static final String SUBFIELD_NAME = "subfield-name";
  private static final String REQUIRED = "required";
  private static final String REPEATABLE = "repeatable";
  private MarcConfig marcConfig;

  public MarcParser() throws IOException, InvalidMarcConfigException {
    marcConfig = new MarcConfig();
  }

  public MarcParser(String configPath) throws IOException, InvalidMarcConfigException {
    marcConfig = new MarcConfig(configPath);
  }

  public JsonObject marcJson2FolioJson(JsonObject inputMarc) throws InvalidMarcJsonException {
    this.validate(inputMarc);
    Map<String,JsonObject> instanceMap = this.extractFolioEntriesFromMarcFields(inputMarc.getJsonArray(FIELDS));
    return this.parse(instanceMap);
  }

  private void validate(JsonObject marc) throws InvalidMarcJsonException {
//    throw new InvalidMarcJsonException();
  }

  private Map<String,JsonObject> extractFolioEntriesFromMarcFields(JsonArray marcFieldsInput) {
    Map<String, JsonObject> instanceMap = this.createConfiguredInitializedInstanceMap();
    return this.mapMarcFieldsToInstanceMap(marcFieldsInput, instanceMap);
  }

  private Map<String,JsonObject> createConfiguredInitializedInstanceMap() {
    Map<String, JsonObject> instanceMap = new LinkedHashMap<>();
    JsonArray configuredInstanceFields = marcConfig.getConfig().getJsonArray(INSTANCE_FIELDS);
    for (Object o : configuredInstanceFields) {
      if (o instanceof JsonObject) {
        JsonObject instanceFieldConfig = (JsonObject) o;
        JsonObject data = new JsonObject();
        data.put(REQUIRED, instanceFieldConfig.getBoolean(REQUIRED));
        data.put(REPEATABLE, instanceFieldConfig.getBoolean(REPEATABLE));
        if (instanceFieldConfig.containsKey(SUBFIELD_NAME)) {
          data.put(SUBFIELD_NAME, instanceFieldConfig.getString(SUBFIELD_NAME));
        }
        data.put(FIELDS, new JsonArray());
        instanceMap.put(
          instanceFieldConfig.getString("name"), data);
      }
    }
    return instanceMap;
  }

  private Map<String,JsonObject> mapMarcFieldsToInstanceMap(JsonArray marcFieldsInput,
                                                            Map<String,JsonObject> instanceMap) {
    JsonObject marcFieldMapping = marcConfig.getConfig().getJsonObject(MARC_FIELDS);
    for (Object o : marcFieldsInput) {
      if (o instanceof JsonObject) {
        JsonObject jo = (JsonObject) o;
        for (String marcNum : jo.fieldNames()) {
          instanceMap.get(marcFieldMapping.getString(marcNum)).getJsonArray(FIELDS).add(jo);
        }
      }
    }
    return instanceMap;
  }

  private JsonObject parse(Map<String,JsonObject> instanceMap) {
    JsonObject output = new JsonObject();
    for (Map.Entry<String,JsonObject> entry : instanceMap.entrySet()) {
      System.out.println(entry.getKey() + ", " + entry.getValue());

      if (entry.getValue().getBoolean(REPEATABLE)) {
        // TODO: parse repeatable
      } else {
        List<String> subfields = new ArrayList<>();
        JsonArray fields = entry.getValue().getJsonArray(FIELDS);
        if (!fields.isEmpty()) {
          String fieldName = fields.getJsonObject(0).fieldNames().iterator().next();
          for (Object o : fields.getJsonObject(0).getJsonObject(fieldName).getJsonArray(
            (SUBFIELDS))) {
            if (o instanceof JsonObject) {
              JsonObject subfield = (JsonObject) o;
              String subfieldName = subfield.fieldNames().iterator().next();
              subfields.add(subfield.getString(subfieldName));
            }
          }
        }
        output.put(entry.getKey(), String.join(" ", subfields));
      }
    }

    return output;
  }

  private boolean isControlNumber(String marcNum, JsonObject jo) {
    return (jo.getValue(marcNum).getClass() == String.class) &&
      Integer.parseInt(marcNum) < 10;
  }

}
