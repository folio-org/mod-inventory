package org.folio.inventory.domain;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class Classification {
  // JSON property names
  public static final String CLASSIFICATION_NUMBER_KEY = "classificationNumber";
  public static final String CLASSIFICATION_TYPE_ID_KEY = "classificationTypeId";

  public final String classificationNumber;
  public final String classificationTypeId;

  public Classification(String classificationTypeId, String classificationNumber) {
    this.classificationTypeId = classificationTypeId;
    this.classificationNumber = classificationNumber;
  }

  public Classification(JsonObject json) {
    this(json.getString(CLASSIFICATION_TYPE_ID_KEY),
         json.getString(CLASSIFICATION_NUMBER_KEY));
  }

}
