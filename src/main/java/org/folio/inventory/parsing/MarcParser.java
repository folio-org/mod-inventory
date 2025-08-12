package org.folio.inventory.parsing;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.exceptions.InvalidMarcJsonException;
import org.folio.inventory.parsing.config.MarcConfig;
import org.folio.inventory.exceptions.InvalidMarcConfigException;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MarcParser {

  private static final String FIELDS = "fields";
  private static final String SUBFIELDS = "subfields";
  private static final String VALUE = "value";
  private static final String TYPE = "type";
  private static final String NAME = "name";
  private static final String MARC_FIELDS = "marc-fields";
  private static final String INSTANCE_FIELDS = "instance-fields";
  private static final String IDENTIFIER_TYPES = "identifier-types";
  private static final String SUBFIELD_NAME = "subfield-name";
  private static final String REQUIRED = "required";
  private static final String REPEATABLE = "repeatable";
  private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());
  private MarcConfig marcConfig;

  public MarcParser() throws IOException, InvalidMarcConfigException {
    marcConfig = new MarcConfig();
  }

  public MarcParser(String configPath) throws IOException, InvalidMarcConfigException {
    marcConfig = new MarcConfig(configPath);
  }

  public JsonObject marcJson2FolioJson(JsonObject inputMarc) throws InvalidMarcJsonException {
    validate(inputMarc);
    Map<String,JsonObject> instanceMap = extractFolioEntriesFromMarcFields(inputMarc.getJsonArray(FIELDS));
    return parse(instanceMap);
  }

  private Map<String,JsonObject> extractFolioEntriesFromMarcFields(JsonArray marcFieldsInput) {
    Map<String, JsonObject> instanceMap = createConfiguredInitializedInstanceMap();
    return mapMarcFieldsToInstanceMap(marcFieldsInput, instanceMap);
  }

  private Map<String,JsonObject> createConfiguredInitializedInstanceMap() {
    Map<String, JsonObject> instanceMap = new LinkedHashMap<>();
    JsonArray configuredInstanceFields = marcConfig.getConfig().getJsonArray(INSTANCE_FIELDS);
    for (Object o : configuredInstanceFields) {
      if (!(o instanceof JsonObject)) {
        continue;
      }
      JsonObject instanceFieldConfig = (JsonObject) o;
      JsonObject data = new JsonObject();
      data.put(REQUIRED, instanceFieldConfig.getBoolean(REQUIRED));
      data.put(REPEATABLE, instanceFieldConfig.getBoolean(REPEATABLE));
      if (instanceFieldConfig.containsKey(SUBFIELD_NAME)) {
        data.put(SUBFIELD_NAME, instanceFieldConfig.getString(SUBFIELD_NAME));
      }
      data.put(FIELDS, new JsonArray());
      instanceMap.put(instanceFieldConfig.getString(NAME), data);
    }
    return instanceMap;
  }

  private Map<String,JsonObject> mapMarcFieldsToInstanceMap(JsonArray marcFieldsInput,
                                                            Map<String,JsonObject> instanceMap) {
    JsonObject marcFieldMapping = marcConfig.getConfig().getJsonObject(MARC_FIELDS);
    for (Object o : marcFieldsInput) {
      if (!(o instanceof JsonObject)) {
        continue;
      }
      JsonObject jo = (JsonObject) o;
      String marcNum = jo.fieldNames().iterator().next();
      if (marcFieldMapping.getString(marcNum) == null) {
        LOGGER.debug(String.format("MARC field %s not found in config and ignored...", marcNum));
      } else {
        instanceMap.get(marcFieldMapping.getString(marcNum)).getJsonArray(FIELDS).add(jo);
      }
    }
    return instanceMap;
  }

  private JsonObject parse(Map<String,JsonObject> instanceMap) {
    JsonObject output = new JsonObject();
    for (Map.Entry<String,JsonObject> entry : instanceMap.entrySet()) {
      if (entry.getValue().getBoolean(REPEATABLE)) {
        parseRepeatable(entry, output);
      } else {
        parseNonRepeatable(entry, output);
      }
    }
    return output;
  }

  private void parseRepeatable(Map.Entry<String, JsonObject> entry, JsonObject output) {
    JsonArray fields = entry.getValue().getJsonArray(FIELDS);
    JsonArray outputArray = new JsonArray();
    for (Object obj : fields) {
      if (obj instanceof JsonObject) {
        JsonObject outputObject = new JsonObject();
        List<String> subfields = new ArrayList<>();
        JsonObject field = (JsonObject) obj;
        String fieldName = field.fieldNames().iterator().next();
        if (isControlNumber(fieldName, field)) {
          outputObject.put(VALUE, field.getString(fieldName));
          putTypeIfConfiguredAsIdentifierType(fieldName, outputObject);
          outputArray.add(outputObject);
          continue;
        }
        for (Object o : field.getJsonObject(fieldName).getJsonArray(SUBFIELDS)) {
          if (!(o instanceof JsonObject)) {
            continue;
          }
          JsonObject subfield = (JsonObject) o;
          String subfieldName = subfield.fieldNames().iterator().next();
          subfields.add(subfield.getString(subfieldName));
        }
        outputObject.put(VALUE, String.join(" ", subfields));
        putTypeIfConfiguredAsIdentifierType(fieldName, outputObject);
        outputArray.add(outputObject);
      }
    }
    output.put(entry.getKey(), outputArray);
  }

  private void putTypeIfConfiguredAsIdentifierType(String fieldName, JsonObject outputObject) {
    if (marcConfig.getConfig().getJsonObject(IDENTIFIER_TYPES).containsKey(fieldName)) {
      outputObject.put(TYPE, marcConfig.getConfig().getJsonObject(IDENTIFIER_TYPES).getString(fieldName));
    }
  }

  private void parseNonRepeatable(Map.Entry<String, JsonObject> entry, JsonObject output) {
    List<String> subfields = new ArrayList<>();
    JsonArray fields = entry.getValue().getJsonArray(FIELDS);
    if (!fields.isEmpty()) {
      String fieldName = fields.getJsonObject(0).fieldNames().iterator().next();
      for (Object o : fields.getJsonObject(0).getJsonObject(fieldName).getJsonArray(
        (SUBFIELDS))) {
        if (!(o instanceof JsonObject)) {
          continue;
        }
        JsonObject subfield = (JsonObject) o;
        String subfieldName = subfield.fieldNames().iterator().next();
        subfields.add(subfield.getString(subfieldName));
      }
    }
    output.put(entry.getKey(), String.join(" ", subfields));
  }

  private boolean isControlNumber(String marcNum, JsonObject jo) {
    return (jo.getValue(marcNum).getClass() == String.class) &&
      Integer.parseInt(marcNum) < 10;
  }

  private void validate(JsonObject marc) throws InvalidMarcJsonException {
    if (!marc.containsKey(FIELDS)) {
      throw new InvalidMarcJsonException("No key 'fields' found in MARC file...");
    }
    if (!(marc.getValue(FIELDS) instanceof JsonArray)) {
      throw new InvalidMarcJsonException("Value at key 'fields' not a JsonArray...");
    }
    JsonArray fields = (JsonArray) marc.getValue(FIELDS);
    for (Object field : fields) {
      if (!(field instanceof JsonObject)) {
        throw new InvalidMarcJsonException("Array 'fields' contains item of type other than JsonObject...");
      }
      if (((JsonObject) field).isEmpty()) {
        throw new InvalidMarcJsonException("Array 'fields' contains an empty JsonObject...");
      }
    }
  }
}
