package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public record Note(String instanceNoteTypeId, String note, Boolean staffOnly) {
  public static final String INSTANCE_NOTE_TYPE_ID_KEY = "instanceNoteTypeId";
  public static final String NOTE_KEY = "note";
  public static final String STAFF_ONLY_KEY = "staffOnly";

  public Note(JsonObject json) {
    this(json.getString(INSTANCE_NOTE_TYPE_ID_KEY),
      json.getString(NOTE_KEY),
      json.getBoolean(STAFF_ONLY_KEY));
  }
}
