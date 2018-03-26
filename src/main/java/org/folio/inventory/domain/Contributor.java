package org.folio.inventory.domain;

public class Contributor {
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
}
