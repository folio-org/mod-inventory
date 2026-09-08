package org.folio.inventory.dataimport.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;

public final class CancelledJobsIdsCache {

  private static final String EXPIRATION_TIME_PARAMETER = "inventory.cancelled-jobs-cache.expiration.time.minutes";
  private static final String DEFAULT_EXPIRATION_TIME_MINUTES = "1440";

  private static CancelledJobsIdsCache instance = null;
  private final Cache<String, Boolean> cache;

  private CancelledJobsIdsCache() {
    int expirationTimeMinutes = Integer.parseInt(System.getProperty(EXPIRATION_TIME_PARAMETER,
      System.getenv().getOrDefault(EXPIRATION_TIME_PARAMETER, DEFAULT_EXPIRATION_TIME_MINUTES)));

    this.cache = Caffeine.newBuilder()
      .expireAfterWrite(expirationTimeMinutes, TimeUnit.MINUTES)
      .build();
  }

  public static CancelledJobsIdsCache getInstance() {
    return getInstance(false);
  }

  public static synchronized CancelledJobsIdsCache getInstance(boolean returnNew) {
    if (instance == null || returnNew) {
      instance = new CancelledJobsIdsCache();
    }
    return instance;
  }

  /**
   * Puts the specified {@code jobId} into the cache.
   *
   * @param jobId import job id to put into the cache
   */
  public void put(String jobId) {
    cache.put(jobId, Boolean.TRUE);
  }

  /**
   * Checks if the cache contains the specified {@code jobId}.
   *
   * @param jobId import job id to check
   * @return {@code true} if the cache contains the {@code jobId}, {@code false} otherwise
   */
  public boolean contains(String jobId) {
    return cache.asMap().containsKey(jobId);
  }
}
