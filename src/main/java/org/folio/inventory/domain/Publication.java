package org.folio.inventory.domain;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class Publication {
  // JSON property names
  public static final String PUBLISHER = "publisher";
  public static final String PLACE = "place";
  public static final String DATE_OF_PUBLICATION = "dateOfPublication";

  public final String publisher;
  public final String place;
  public final String dateOfPublication;

  public Publication(String publisher, String place, String dateOfPublication) {
    this.publisher = publisher;
    this.place = place;
    this.dateOfPublication = dateOfPublication;
  }

  public Publication(JsonObject json) {
    this(json.getString(PUBLISHER),
         json.getString(PLACE),
         json.getString(DATE_OF_PUBLICATION));
  }

}
