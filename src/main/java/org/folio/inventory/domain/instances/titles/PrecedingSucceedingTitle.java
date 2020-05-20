package org.folio.inventory.domain.instances.titles;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class PrecedingSucceedingTitle {
  public static final String PRECEDING_INSTANCE_ID_KEY = "precedingInstanceId";
  public static final String SUCCEEDING_INSTANCE_ID_KEY = "succeedingInstanceId";
  public static final String TITLE_KEY = "title";
  public static final String HRID_KEY = "hrid";
  public static final String IDENTIFIERS_KEY = "identifiers";

  public final String id;
  public final String precedingInstanceId;
  public final String succeedingInstanceId;
  public final String title;
  public final String hrid;
  public final JsonArray identifiers;

  public PrecedingSucceedingTitle(String id, String precedingInstanceId,
    String succeedingInstanceId, String title, String hrid, JsonArray identifiers) {

    this.id = id;
    this.precedingInstanceId = precedingInstanceId;
    this.succeedingInstanceId = succeedingInstanceId;
    this.title = title;
    this.hrid = hrid;
    this.identifiers = identifiers;
  }

  public static PrecedingSucceedingTitle from(JsonObject rel) {
    return new PrecedingSucceedingTitle(rel.getString("id"),
         rel.getString(PRECEDING_INSTANCE_ID_KEY),
         rel.getString(SUCCEEDING_INSTANCE_ID_KEY),
         rel.getString(TITLE_KEY),
         rel.getString(HRID_KEY),
         rel.getJsonArray(IDENTIFIERS_KEY));
  }

  public static PrecedingSucceedingTitle from(JsonObject rel, String title, String hrid,
    JsonArray identifiers) {
    return new PrecedingSucceedingTitle(rel.getString("id"),
      rel.getString(PRECEDING_INSTANCE_ID_KEY),
      rel.getString(SUCCEEDING_INSTANCE_ID_KEY),
      title, hrid, identifiers);
  }

  public JsonObject toPrecedingTitleJson() {
    return toJson(PRECEDING_INSTANCE_ID_KEY, precedingInstanceId);
  }

  public JsonObject toSucceedingTitleJson() {
    return toJson(SUCCEEDING_INSTANCE_ID_KEY, succeedingInstanceId);
  }

  private JsonObject toJson(String succeedingInstanceIdKey, String succeedingInstanceId) {
    JsonObject json = new JsonObject();
    putIfNotNull(json, "id", id);
    putIfNotNull(json, TITLE_KEY, title);
    putIfNotNull(json, HRID_KEY, hrid);
    putIfNotNull(json, succeedingInstanceIdKey, succeedingInstanceId);
    if (identifiers != null) {
      json.put(IDENTIFIERS_KEY, identifiers);
    }
    return json;
  }

  private void putIfNotNull(JsonObject instanceToSend, String propertyName, String propertyValue) {
    if (propertyValue != null) {
      instanceToSend.put(propertyName, propertyValue);
    }
  }
}
