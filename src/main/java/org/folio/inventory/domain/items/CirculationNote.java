/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.folio.inventory.domain.items;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.user.User;

/**
 *
 * @author ne
 */
public record CirculationNote(String id, String noteType, String note, Boolean staffOnly, User source, String date) {
  public static final String ID_KEY = "id";
  public static final String NOTE_TYPE_KEY = "noteType";
  public static final String NOTE_KEY = "note";
  public static final String STAFF_ONLY_KEY = "staffOnly";
  public static final String SOURCE_KEY = "source";
  public static final String DATE_KEY = "date";

  public CirculationNote(JsonObject json) {
    this(json.getString(ID_KEY),
      json.getString(NOTE_TYPE_KEY),
      json.getString(NOTE_KEY),
      json.getBoolean(STAFF_ONLY_KEY),
      new User(json.getJsonObject(SOURCE_KEY)),
      json.getString(DATE_KEY)
    );
  }

  public CirculationNote withId(String id) {
    return new CirculationNote(id, noteType, note, staffOnly, source, date);
  }

  public CirculationNote withSource(User source) {
    return new CirculationNote(id, noteType, note, staffOnly, source, date);
  }

  public CirculationNote withDate(String date) {
    return new CirculationNote(id, noteType, note, staffOnly, source, date);
  }

  public CirculationNote withNoteType(String noteType) {
    return new CirculationNote(id, noteType, note, staffOnly, source, date);
  }
}
