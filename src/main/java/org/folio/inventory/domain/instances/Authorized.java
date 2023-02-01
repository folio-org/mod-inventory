package org.folio.inventory.domain.instances;

public abstract class Authorized {

  protected static final String AUTHORITY_ID_KEY = "authorityId";

  public final String authorityId;

  protected Authorized(String authorityId) {
    this.authorityId = authorityId;
  }
}
