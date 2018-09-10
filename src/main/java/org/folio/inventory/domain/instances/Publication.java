package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class Publication {
  // JSON property names
  public static final String PUBLISHER_KEY = "publisher";
  public static final String PLACE_KEY = "place";
  public static final String DATE_OF_PUBLICATION_KEY = "dateOfPublication";
  public static final String ROLE = "role";

  public final String publisher;
  public final String place;
  public final String dateOfPublication;
  public final String role;

  public Publication(String publisher, String place, String dateOfPublication, String role) {
    this.publisher = publisher;
    this.place = place;
    this.dateOfPublication = dateOfPublication;
    this.role = role;
  }

  public Publication(JsonObject json) {
    this(json.getString(PUBLISHER_KEY),
         json.getString(PLACE_KEY),
         json.getString(DATE_OF_PUBLICATION_KEY),
         json.getString(ROLE));
  }
}
