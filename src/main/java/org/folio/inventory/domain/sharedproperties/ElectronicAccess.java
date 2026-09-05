package org.folio.inventory.domain.sharedproperties;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public record ElectronicAccess(String uri, String linkText, String materialsSpecification, String publicNote,
                               String relationshipId) {
  // JSON property names
  public static final String URI_KEY = "uri";
  public static final String LINK_TEXT_KEY = "linkText";
  public static final String MATERIALS_SPECIFICATION_KEY = "materialsSpecification";
  public static final String PUBLIC_NOTE_KEY = "publicNote";
  public static final String RELATIONSHIP_ID_KEY = "relationshipId";

  public ElectronicAccess(JsonObject json) {
    this(json.getString(URI_KEY),
      json.getString(LINK_TEXT_KEY),
      json.getString(MATERIALS_SPECIFICATION_KEY),
      json.getString(PUBLIC_NOTE_KEY),
      json.getString(RELATIONSHIP_ID_KEY));
  }
}
