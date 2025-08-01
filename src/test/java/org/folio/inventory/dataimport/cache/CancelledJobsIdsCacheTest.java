package org.folio.inventory.dataimport.cache;

import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CancelledJobsIdsCacheTest {

  private CancelledJobsIdsCache cache;

  @Before
  public void setUp() {
    cache = new CancelledJobsIdsCache();
  }

  @Test
  public void shouldIdAddToCache() {
    UUID jobId = UUID.randomUUID();
    cache.put(jobId);
    assertTrue(cache.contains(jobId));
  }

  @Test
  public void shouldReturnFalseForNonExistentId() {
    UUID jobId = UUID.randomUUID();
    assertFalse(cache.contains(jobId));
  }

  @Test
  public void shouldThrowExceptionIfJobIdIsNull() {
    assertThrows(NullPointerException.class, () -> cache.contains(null));
  }

}
