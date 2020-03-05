package org.folio.inventory.domain.instances.titles;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class PrecedingSucceedingTitle {
  public static final String PRECEDING_INSTANCE_INSTANCE_ID_KEY = "precedingInstanceId";
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

  public PrecedingSucceedingTitle(JsonObject rel) {
    this(rel.getString("id"),
         rel.getString(PRECEDING_INSTANCE_INSTANCE_ID_KEY),
         rel.getString(SUCCEEDING_INSTANCE_ID_KEY),
         rel.getString(TITLE_KEY),
         rel.getString(HRID_KEY),
         rel.getJsonArray(IDENTIFIERS_KEY)
      );
  }

  public PrecedingSucceedingTitle(JsonObject rel, String title, String hrid,
    JsonArray identifiers) {
    this(rel.getString("id"), rel.getString(PRECEDING_INSTANCE_INSTANCE_ID_KEY),
      rel.getString(SUCCEEDING_INSTANCE_ID_KEY), title, hrid, identifiers);
  }
}
