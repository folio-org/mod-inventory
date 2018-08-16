package org.folio.inventory.domain;

import io.vertx.core.json.JsonObject;

public class Contributor {
  // JSON property names
  public static final String CONTRIBUTOR_NAME_TYPE_ID_KEY = "contributorNameTypeId";
  public static final String NAME_KEY = "name";
  public static final String CONTRIBUTOR_TYPE_ID_KEY = "contributorTypeId";
  public static final String CONTRIBUTOR_TYPE_TEXT_KEY = "contributorTypeText";

  public final String contributorNameTypeId;
  public final String name;
  public final String contributorTypeId;
  public final String contributorTypeText;

  public Contributor(String contributorNameTypeId, String name, String contributorTypeId, String contributorTypeText) {
    this.contributorNameTypeId = contributorNameTypeId;
    this.name = name;
    this.contributorTypeId = contributorTypeId;
    this.contributorTypeText = contributorTypeText;
  }

  public Contributor(JsonObject json) {
    this(json.getString(CONTRIBUTOR_NAME_TYPE_ID_KEY),
         json.getString(NAME_KEY),
         json.getString(CONTRIBUTOR_TYPE_ID_KEY),
         json.getString(CONTRIBUTOR_TYPE_TEXT_KEY));
  }

}
