package org.folio.inventory.dataimport.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CancelledJobsIdsCacheTest {

  private CancelledJobsIdsCache cache;

  @BeforeEach
  void setUp() {
    cache = new CancelledJobsIdsCache();
  }

  @Test
  void shouldIdAddToCache() {
    var jobId = UUID.randomUUID().toString();
    cache.put(jobId);
    assertTrue(cache.contains(jobId));
  }

  @Test
  void shouldReturnFalseForNonExistentId() {
    var jobId = UUID.randomUUID().toString();
    assertFalse(cache.contains(jobId));
  }

  @Test
  void shouldThrowExceptionIfJobIdIsNull() {
    assertThrows(NullPointerException.class, () -> cache.contains(null));
  }
}
