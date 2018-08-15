package org.folio.inventory.domain;

/**
 *
 * @author ne
 */
public class Classification {
  public static final String CLASSIFICATION_NUMBER = "classificationNumber";
  public static final String CLASSIFICATION_TYPE_ID = "classificationTypeId";

  public final String classificationNumber;
  public final String classificationTypeId;

  public Classification(String classificationTypeId, String classificationNumber) {
    this.classificationTypeId = classificationTypeId;
    this.classificationNumber = classificationNumber;
  }
}
