package org.folio.inventory.dataimport.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class CancelledJobsIdsCache {

  public static final String EXPIRATION_TIME_PARAMETER = "inventory.cancelled-jobs-cache.expiration.time.minutes";
  private static final String DEFAULT_EXPIRATION_TIME_MINUTES = "1440";

  private final Cache<UUID, Boolean> cache;

  public CancelledJobsIdsCache() {
    int expirationTimeMinutes = Integer.parseInt(System.getProperty(EXPIRATION_TIME_PARAMETER,
        System.getenv().getOrDefault(EXPIRATION_TIME_PARAMETER, DEFAULT_EXPIRATION_TIME_MINUTES)));

    this.cache = Caffeine.newBuilder()
      .expireAfterWrite(expirationTimeMinutes, TimeUnit.MINUTES)
      .build();
  }

  /**
   * Puts the specified {@code jobId} into the cache.
   *
   * @param jobId the UUID to put into the cache
   */
  public void put(UUID jobId) {
    cache.put(jobId, Boolean.TRUE);
  }

  /**
   * Checks if the cache contains the specified {@code jobId}.
   *
   * @param jobId the job UUID to check
   * @return {@code true} if the cache contains the {@code jobId}, {@code false} otherwise
   */
  public boolean contains(UUID jobId) {
    return cache.asMap().containsKey(jobId);
  }

}
