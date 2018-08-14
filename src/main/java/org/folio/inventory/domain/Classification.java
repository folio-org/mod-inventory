package org.folio.inventory.domain;

/**
 *
 * @author ne
 */
public class Classification {
  public final String classificationNumber;
  public final String classificationTypeId;

  public Classification(String classificationTypeId, String classificationNumber) {
    this.classificationTypeId = classificationTypeId;
    this.classificationNumber = classificationNumber;
  }
}
