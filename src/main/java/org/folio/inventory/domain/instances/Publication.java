package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public record Publication(String publisher, String place, String dateOfPublication, String role) {
  // JSON property names
  public static final String PUBLISHER_KEY = "publisher";
  public static final String PLACE_KEY = "place";
  public static final String DATE_OF_PUBLICATION_KEY = "dateOfPublication";
  public static final String ROLE_KEY = "role";

  public Publication(JsonObject json) {
    this(json.getString(PUBLISHER_KEY),
      json.getString(PLACE_KEY),
      json.getString(DATE_OF_PUBLICATION_KEY),
      json.getString(ROLE_KEY));
  }
}
