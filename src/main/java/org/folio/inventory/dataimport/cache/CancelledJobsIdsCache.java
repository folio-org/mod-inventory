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

  public void put(UUID uuid) {
    cache.put(uuid, Boolean.TRUE);
  }

  public boolean contains(UUID uuid) {
    return cache.asMap().containsKey(uuid);
  }

}
