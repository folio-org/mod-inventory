package org.folio.inventory.dataimport.util;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

/**
 * Non-Caffeine snapshot of {@link AdditionalFieldsUtil}'s internal parsed-record-content cache metrics, so
 * consumers can read cache statistics without depending on Caffeine's {@link CacheStats} type.
 *
 * @param requestCount total of hits and misses
 * @param hitCount     number of times a cache lookup was served from cache
 * @param missCount    number of times a cache lookup was not found in cache
 * @param loadCount    number of times a value was computed and loaded into the cache
 */
public record AdditionalFieldsUtilCacheStats(long requestCount, long hitCount, long missCount, long loadCount) {

  static AdditionalFieldsUtilCacheStats fromCaffeine(CacheStats cacheStats) {
    return new AdditionalFieldsUtilCacheStats(cacheStats.requestCount(), cacheStats.hitCount(),
      cacheStats.missCount(), cacheStats.loadCount());
  }

  /**
   * Returns the difference between this snapshot and an earlier one, mirroring Caffeine's own
   * {@code CacheStats.minus(...)} so callers taking a "before" snapshot can compute deltas the same way.
   *
   * @param other earlier snapshot to subtract
   * @return a new snapshot holding the non-negative per-field differences
   */
  public AdditionalFieldsUtilCacheStats minus(AdditionalFieldsUtilCacheStats other) {
    return new AdditionalFieldsUtilCacheStats(
      Math.max(0, requestCount - other.requestCount()),
      Math.max(0, hitCount - other.hitCount()),
      Math.max(0, missCount - other.missCount()),
      Math.max(0, loadCount - other.loadCount()));
  }
}
