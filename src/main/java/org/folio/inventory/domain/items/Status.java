/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventory.domain.items;

import java.util.Objects;

/**
 * @author ne
 */
public class Status {
  private final ItemStatusName name;
  private final String date;

  public Status(ItemStatusName name) {
    this.name = name;
    this.date = null;
  }

  public Status(ItemStatusName itemStatusName, String date) {
    this.name = Objects.requireNonNull(itemStatusName, "Status name is required");
    this.date = date;
  }

  public ItemStatusName getName() {
    return name;
  }

  public String getDate() {
    return date;
  }
}
