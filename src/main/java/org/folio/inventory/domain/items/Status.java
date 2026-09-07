/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.folio.inventory.domain.items;

import java.util.Objects;

public record Status(ItemStatusName name, String date) {
  public Status(ItemStatusName name) {
    this(name, null);
  }

  public Status(ItemStatusName name, String date) {
    this.name = Objects.requireNonNull(name, "Status name is required");
    this.date = date;
  }
}
