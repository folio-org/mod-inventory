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
  public static final String ID_KEY = "id";
  public static final String NOTE_TYPE_KEY = "noteType";
  public static final String NOTE_KEY = "note";
  public static final String STAFF_ONLY_KEY = "staffOnly";
  public static final String SOURCE_KEY = "source";
  public static final String DATE_KEY = "date";

  private final String id;
  private final String noteType;
  private final String note;
  private final Boolean staffOnly;
  private final User source;
  private final String date;

  public CirculationNote (String id,
                          String noteType,
                          String note,
                          Boolean staffOnly,
                          User source,
                          String date) {
    this.id = id;
    this.noteType = noteType;
    this.note = note;
    this.staffOnly = staffOnly;
    this.source = source;
    this.date = date;
  }

  public CirculationNote (JsonObject json) {
    this(json.getString(ID_KEY),
      json.getString(NOTE_TYPE_KEY),
      json.getString(NOTE_KEY),
      json.getBoolean(STAFF_ONLY_KEY),
      new User(json.getJsonObject(SOURCE_KEY)),
      json.getString(DATE_KEY)
    );
  }

  public String getId() {
    return id;
  }

  public String getNoteType() {
    return noteType;
  }

  public String getNote() {
    return note;
  }

  public Boolean getStaffOnly() {
    return staffOnly;
  }

  public User getSource() {
    return source;
  }

  public String getDate() {
    return date;
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
