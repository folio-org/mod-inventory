/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventory.domain.items;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class Status {
  public static final String NAME_KEY = "name";
  public static final String DATE_KEY = "date";

  private JsonObject json = new JsonObject();

  public Status (String name) {
    json.put(NAME_KEY,name);
  }

  public Status (JsonObject status) {
    this.json = status;
  }

  public JsonObject getJson() {
    return json;
  }

  public String getName () {
    return json.getString(NAME_KEY);
  }

  public String getDate () {
    return json.getString(DATE_KEY);
  }

}
