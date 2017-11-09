package org.folio.inventory.domain

class Creator {
  public final String creatorTypeId
  public final String name

  public Creator(String creatorTypeId, String name) {
    this.creatorTypeId = creatorTypeId
    this.name = name
  }
}
