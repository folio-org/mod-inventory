package org.folio.inventory.domain.items;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StatusTest {

  @Test
  void cannotCreateStatusIfStatusNameIsNull() {
    var ex = assertThrows(NullPointerException.class, () -> new Status(null, "date"));
    assertTrue(ex.getMessage().contains("Status name is required"));
  }
}
