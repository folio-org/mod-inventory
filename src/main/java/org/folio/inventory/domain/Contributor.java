package org.folio.inventory.domain;

public class Contributor {
  public static final String CONTRIBUTOR_NAME_TYPE_ID = "contributorNameTypeId";
  public static final String NAME = "name";
  public static final String CONTRIBUTOR_TYPE_ID = "contributorTypeId";
  public static final String CONTRIBUTOR_TYPE_TEXT = "contributorTypeText";

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
