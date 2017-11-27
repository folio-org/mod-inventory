package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.inventory.domain.Instance;
import java.lang.invoke.MethodHandles;
import java.util.*;

public class MarcParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String FIELDS = "fields";
  private static final String SUBFIELDS = "subfields";
  private static final String TITLE_FIELD = "245";

  public Instance marcJsonToFolioInstance(JsonObject marcEntry) {
    String id = "";
    String title = "";
    String source = "";
    String instanceTypeId = "";

    if (marcEntry.containsKey(FIELDS) && marcEntry.getValue(FIELDS).getClass() == JsonArray.class) {
      JsonArray fieldArray = marcEntry.getJsonArray(FIELDS);
      List<JsonObject> fields = this.extractFromJsonArray(fieldArray);
      title = this.extractTitle(fields);
    } else {
      LOGGER.debug("JsonObject [marcEntry]: no JsonArray found at key 'fields'");
    }
    return new Instance(
      id,
      title,
      new ArrayList<>(),
      source,
      instanceTypeId,
      new ArrayList<>()
    );
  }

  private String extractTitle(List<JsonObject> fields) {
    for (JsonObject json : fields) {
      if (json.containsKey(TITLE_FIELD) && json.getJsonObject(TITLE_FIELD).containsKey(SUBFIELDS) &&
        json.getJsonObject(TITLE_FIELD).getValue(SUBFIELDS).getClass() == JsonArray.class) {
        JsonArray array = json.getJsonObject(TITLE_FIELD).getJsonArray(SUBFIELDS);
        List<JsonObject> subfields = this.extractFromJsonArray(array);
        return this.concatenateTitleValues(subfields);
      }
    }
    return null;
  }

  private String concatenateTitleValues(List<JsonObject> subfields) {
    List<String> subfieldValues = new ArrayList<>();
    List<String> keys = Arrays.asList("a", "b", "c", "f", "g", "h", "s");

    for (String key : keys) {
      String val = this.getStringValueForKey(subfields, key);
      if (val != null) {
        subfieldValues.add(val);
      }
    }
    return String.join(" ", subfieldValues);
  }

  private String getStringValueForKey(List<JsonObject> subfields, String key) {
    for (JsonObject subfield : subfields) {
      if (subfield.containsKey(key) && subfield.getValue(key).getClass() == String.class) {
        return subfield.getString(key);
      }
    }
    return null;
  }

  private List<JsonObject> extractFromJsonArray(JsonArray fieldArray) {
    int nFields = fieldArray.getList().size();
    List<JsonObject> list = new ArrayList<>();
    for (int i=0; i<nFields; i++) {
      if (fieldArray.getValue(i).getClass() == JsonObject.class) {
        list.add(fieldArray.getJsonObject(i));
      } else {
        LOGGER.debug(fieldArray.getValue(i) + ": not of class " + JsonObject.class);
      }
    }
    return list;
  }
}
