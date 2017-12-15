package org.folio.inventory.parsing;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.inventory.exceptions.InvalidMarcJsonException;
import org.folio.inventory.exceptions.MarcParseException;
import org.folio.inventory.parsing.config.MarcConfig;
import org.folio.inventory.exceptions.InvalidMarcConfigException;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parse an instance record in MARC JSON and convert it to a FOLIO JSON record.
 */
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
  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private MarcConfig marcConfig;

  /**
   * Parser with default config.
   * @throws InvalidMarcConfigException  when default config file is invalid
   */
  public MarcParser() throws InvalidMarcConfigException {
    marcConfig = new MarcConfig();
  }

  /**
   * Parser using a specific config file.
   * @param configPath  resource path of the config file.
   * @throws InvalidMarcConfigException  when config file is invalid
   */
  public MarcParser(String configPath) throws InvalidMarcConfigException {
    marcConfig = new MarcConfig(configPath);
  }

  /**
   * Convert the MARC JSON of an instance record to a FOLIO JSON.
   * @param inputMarc  MARC JSON input
   * @return FOLIO JSON output
   * @throws MarcParseException  if the input is invalid
   */
  public JsonObject marcJson2FolioJson(JsonObject inputMarc) throws MarcParseException {
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

  private JsonObject parse(Map<String,JsonObject> instanceMap) throws MarcParseException {
    JsonObject output = new JsonObject();
    for (Map.Entry<String,JsonObject> entry : instanceMap.entrySet()) {
      try {
        if (entry.getValue().getBoolean(REPEATABLE)) {
          parseRepeatable(entry, output);
        } else {
          parseNonRepeatable(entry, output);
        }
      }
      catch (Exception e) {
        throw new MarcParseException(entry.getKey(), e);
      }
    }
    return output;
  }

  /**
   * Return the concatenated content of the subfields.
   * <p>
   * subfields({"subfields":[{"a":"foo"},{"b":"bar"}]}) = "foo bar"
   *
   * @param jsonObject  json to parse
   * @return concatenated subfield values
   */
  private String subfields(JsonObject jsonObject) {
    List<String> subfields = new ArrayList<>();
    for (Object o : jsonObject.getJsonArray(SUBFIELDS)) {
      JsonObject subfield = (JsonObject) o;
      String subfieldName = subfield.fieldNames().iterator().next();
      subfields.add(subfield.getString(subfieldName));
    }
    return String.join(" ", subfields);
  }

  private void parseRepeatable(Map.Entry<String, JsonObject> entry, JsonObject output) {
    JsonArray fields = entry.getValue().getJsonArray(FIELDS);
    JsonArray outputArray = new JsonArray();
    for (Object obj : fields) {
      JsonObject field = (JsonObject) obj;
      String fieldName = field.fieldNames().iterator().next();
      JsonObject outputObject = new JsonObject();
      if (isControlNumber(fieldName, field)) {
        outputObject.put(VALUE, field.getString(fieldName));
      } else {
        outputObject.put(VALUE, subfields(field.getJsonObject(fieldName)));
      }
      putTypeIfConfiguredAsIdentifierType(fieldName, outputObject);
      outputArray.add(outputObject);
    }
    output.put(entry.getKey(), outputArray);
  }

  private void putTypeIfConfiguredAsIdentifierType(String fieldName, JsonObject outputObject) {
    JsonObject identifierTypes = marcConfig.getConfig().getJsonObject(IDENTIFIER_TYPES);
    if (identifierTypes == null) {
      return;
    }
    String type = identifierTypes.getString(fieldName);
    if (type == null) {
      return;
    }
    outputObject.put(TYPE, type);
  }

  private void parseNonRepeatable(Map.Entry<String, JsonObject> entry, JsonObject output) {
    JsonArray fields = entry.getValue().getJsonArray(FIELDS);
    if (fields.isEmpty()) {
      output.put(entry.getKey(), "");
      return;
    }
    /** for example 245 */
    String fieldName = fields.getJsonObject(0).fieldNames().iterator().next();
    JsonObject field = fields.getJsonObject(0).getJsonObject(fieldName);
    output.put(entry.getKey(), subfields(field));
  }

  private boolean isControlNumber(String marcNum, JsonObject jo) {
    return (jo.getValue(marcNum) instanceof String) &&
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
