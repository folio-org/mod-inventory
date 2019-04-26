package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class Contributor {
  // JSON property names
  public static final String CONTRIBUTOR_NAME_TYPE_ID_KEY = "contributorNameTypeId";
  public static final String NAME_KEY = "name";
  public static final String CONTRIBUTOR_TYPE_ID_KEY = "contributorTypeId";
  public static final String CONTRIBUTOR_TYPE_TEXT_KEY = "contributorTypeText";
  public static final String PRIMARY_KEY = "primary";

  public final String contributorNameTypeId;
  public final String name;
  public final String contributorTypeId;
  public final String contributorTypeText;
  public final Boolean primary;

  public Contributor(String contributorNameTypeId, String name, String contributorTypeId, String contributorTypeText, Boolean primary) {
    this.contributorNameTypeId = contributorNameTypeId;
    this.name = name;
    this.contributorTypeId = contributorTypeId;
    this.contributorTypeText = contributorTypeText;
    this.primary = primary;
  }

  public Contributor(JsonObject json) {
    this(json.getString(CONTRIBUTOR_NAME_TYPE_ID_KEY),
         json.getString(NAME_KEY),
         json.getString(CONTRIBUTOR_TYPE_ID_KEY),
         json.getString(CONTRIBUTOR_TYPE_TEXT_KEY),
         json.getBoolean(PRIMARY_KEY));
  }

}
