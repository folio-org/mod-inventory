/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventory.domain.items;

import static org.folio.inventory.support.JsonHelper.getString;
import static org.folio.inventory.support.JsonHelper.includeIfPresent;

import io.vertx.core.json.JsonObject;

/**
 * @author ne
 */
public class Status {
  public static final String NAME_KEY = "name";
  public static final String DATE_KEY = "date";

  private final ItemStatusName name;
  private final String date;

  public Status(ItemStatusName name) {
    this.name = name;
    this.date = null;
  }

  public Status(String name) {
    this(name, null);
  }

  public Status(JsonObject status) {
    this(
      getString(status, NAME_KEY),
      getString(status, DATE_KEY)
    );
  }

  private Status(String itemStatusName, String date) {
    this.name = itemStatusName != null
      ? ItemStatusName.forName(itemStatusName)
      : null;
    this.date = date;
  }

  public JsonObject getJson() {
    JsonObject status = new JsonObject();

    if (name != null) {
      status.put(NAME_KEY, name.value());
    }
    includeIfPresent(status, DATE_KEY, date);

    return status;
  }

  public ItemStatusName getName() {
    return name;
  }

  public String getDate() {
    return date;
  }
}
