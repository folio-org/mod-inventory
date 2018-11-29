package org.folio.inventory.domain.items;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class Note {
  public static final String ITEM_NOTE_TYPE_ID_KEY = "itemNoteTypeId";
  public static final String NOTE_KEY = "note";
  public static final String STAFF_ONLY_KEY = "staffOnly";

  public final String itemNoteTypeId;
  public final String note;
  public final Boolean staffOnly;

  public Note (String itemNoteTypeId, String note, Boolean staffOnly) {
    this.itemNoteTypeId = itemNoteTypeId;
    this.note = note;
    this.staffOnly = staffOnly;
  }

  public Note (JsonObject json) {
    this(json.getString(ITEM_NOTE_TYPE_ID_KEY),
         json.getString(NOTE_KEY),
         json.getBoolean(STAFF_ONLY_KEY));
  }

}




