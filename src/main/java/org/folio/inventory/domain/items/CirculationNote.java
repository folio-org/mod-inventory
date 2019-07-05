/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventory.domain.items;

import org.folio.inventory.domain.user.User;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class CirculationNote {
  public static final String NOTE_TYPE_KEY = "noteType";
  public static final String NOTE_KEY = "note";
  public static final String STAFF_ONLY_KEY = "staffOnly";

  public final String noteType;
  public final String note;
  public final Boolean staffOnly;

  public User source;
  public String date;

  public CirculationNote (String noteType, String note, Boolean staffOnly) {
    this.noteType = noteType;
    this.note = note;
    this.staffOnly = staffOnly;
  }

  public CirculationNote (String noteType, String note, Boolean staffOnly,
                          User source, String date) {
    this.noteType = noteType;
    this.note = note;
    this.staffOnly = staffOnly;
    this.source = source;
    this.date = date;
  }

  public CirculationNote (JsonObject json) {
    this(json.getString(NOTE_TYPE_KEY),
      json.getString(NOTE_KEY),
      json.getBoolean(STAFF_ONLY_KEY));
  }

  public CirculationNote withSource(User source) {
    return new CirculationNote(noteType, note, staffOnly, source, date);
  }

  public CirculationNote withDate(String date) {
    return new CirculationNote(noteType, note, staffOnly, source, date);
  }
}
