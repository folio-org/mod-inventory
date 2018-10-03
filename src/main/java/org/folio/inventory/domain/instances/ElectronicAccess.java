package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class ElectronicAccess {
    // JSON property names
  public static final String URI_KEY = "uri";
  public static final String LINK_TEXT_KEY = "linkText";
  public static final String MATERIALS_SPECIFICATION_KEY = "materialsSpecification";
  public static final String PUBLIC_NOTE_KEY = "publicNote";
  public static final String RELATIONSHIP_ID_KEY = "relationshipId";

  public final String uri;
  public final String linkText;
  public final String materialsSpecification;
  public final String publicNote;
  public final String relationshipId;

  public ElectronicAccess(
          String uri,
          String linkText,
          String materialsSpecification,
          String publicNote,
          String relationshipId) {
    this.uri = uri;
    this.linkText = linkText;
    this.materialsSpecification = materialsSpecification;
    this.publicNote = publicNote;
    this.relationshipId = relationshipId;
  }

    public ElectronicAccess(JsonObject json) {
    this(json.getString(URI_KEY),
         json.getString(LINK_TEXT_KEY),
         json.getString(MATERIALS_SPECIFICATION_KEY),
         json.getString(PUBLIC_NOTE_KEY),
         json.getString(RELATIONSHIP_ID_KEY));
  }

}
