package org.folio.inventory.domain;

public class Contributor {
  public final String contributorNameTypeId;
  public final String name;

  public Contributor(String contributorNameTypeId, String name) {
    this.contributorNameTypeId = contributorNameTypeId;
    this.name = name;
  }
}
