package org.folio.inventory.dataimport.cache;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.MappingProfile;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;

/**
 * Cache that memoizes, per {@link MappingProfile} id, whether the profile contains at least one
 * MARC-modification rule with action {@link MarcMappingDetail.Action#DELETE} targeting field "999"
 * and at least one of subfields $i, $s or wildcard {@code *} (all other subfields are ignored).
 * <p>
 * Keyed by MappingProfile id (stable UUID). Value is {@link Boolean}. Used by
 * {@code AbstractModifyEventHandler} to skip the {@code externalIdsHolder} synchronization step
 * when the profile does not delete 999 (i.e., typical case), avoiding the JSON decode/encode cost.
 * <p>
 * Uses the same {@link Caffeine} configuration style as {@link MappingMetadataCache}
 * (expireAfterAccess, {@link AsyncCache} with a Vertx-bound executor). Expiration is configurable
 * via the {@value #CACHE_EXPIRATION_TIME_ENV} system property (seconds);
 * default {@value #DEFAULT_EXPIRATION_TIME_SECONDS} seconds.
 */
public final class DeleteRuleFor999FieldCache {

  private static final Logger LOGGER = LogManager.getLogger(DeleteRuleFor999FieldCache.class);
  private static final String CACHE_EXPIRATION_TIME_ENV = "inventory.delete-999-rule-cache.expiration.time.seconds";
  private static final String DEFAULT_EXPIRATION_TIME_SECONDS = "3600";
  private static final String TAG_999 = "999";
  /**
   * Subfield codes that make the DELETE-999 rule relevant to {@code externalIdsHolder} sync:
   * {@code i} (instance id backlink), {@code s} (matched id / SRS record id), and {@code *}
   * (wildcard - whole field removal, which implicitly includes $i and $s).
   * Any DELETE-999 rule targeting only other subfields (e.g. $l, $t) is ignored because it
   * does not affect the 999$i value that the holder mirrors.
   */
  private static final List<String> TARGET_999_SUBFIELDS = List.of("i", "s", "*");

  private static DeleteRuleFor999FieldCache instance = null;

  private final AsyncCache<String, Boolean> cache;

  public DeleteRuleFor999FieldCache(Vertx vertx, long cacheExpirationTimeSeconds) {
    this.cache = Caffeine.newBuilder()
      .expireAfterAccess(cacheExpirationTimeSeconds, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
    LOGGER.info("DeleteRuleFor999FieldCache initialized with expireAfterAccess={}s", cacheExpirationTimeSeconds);
  }

  public static DeleteRuleFor999FieldCache getInstance(Vertx vertx) {
    return getInstance(vertx, false);
  }

  /**
   * Used for testing.
   */
  public static synchronized DeleteRuleFor999FieldCache getInstance(Vertx vertx, boolean returnNew) {
    if (instance == null || returnNew) {
      instance = new DeleteRuleFor999FieldCache(vertx, resolveCacheExpirationSeconds());
    }
    return instance;
  }

  /**
   * Returns a {@link Future} completing with {@code true} when the given {@link MappingProfile}
   * contains a DELETE rule for MARC field 999 targeting $i, $s or wildcard {@code *} subfield.
   * Rules that only touch other 999 subfields (e.g. $l, $t) are ignored.
   * Result is memoized by profile id.
   */
  public Future<Boolean> containsDeleteRuleFor999Field(MappingProfile mappingProfile) {
    if (mappingProfile == null) {
      return Future.succeededFuture(false);
    }
    String key = mappingProfile.getId();
    if (StringUtils.isBlank(key)) {
      return Future.succeededFuture(computeContainsDeleteRuleFor999Field(mappingProfile));
    }
    return Future.fromCompletionStage(cache.get(key,
      (k, executor) -> CompletableFuture.completedFuture(computeContainsDeleteRuleFor999Field(mappingProfile))));
  }

  private static boolean computeContainsDeleteRuleFor999Field(MappingProfile mappingProfile) {
    if (mappingProfile.getMappingDetails() == null
        || mappingProfile.getMappingDetails().getMarcMappingDetails() == null) {
      return false;
    }
    for (MarcMappingDetail detail : mappingProfile.getMappingDetails().getMarcMappingDetails()) {
      if (detail == null || detail.getAction() != MarcMappingDetail.Action.DELETE || detail.getField() == null) {
        continue;
      }
      if (!TAG_999.equals(detail.getField().getField())) {
        continue;
      }
      if (ruleTargetsRelevantSubfield(detail)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code true} only if the DELETE rule for tag 999 targets at least one of
   * {@link #TARGET_999_SUBFIELDS} ($i, $s or wildcard *). Rules targeting only other subfields
   * (e.g. $l, $t) are considered irrelevant to the {@code externalIdsHolder} synchronization.
   * A rule with no subfields specified is treated as targeting the whole field (wildcard-equivalent)
   * and therefore returns {@code true}.
   */
  private static boolean ruleTargetsRelevantSubfield(MarcMappingDetail detail) {
    List<MarcSubfield> subfields = detail.getField().getSubfields();
    if (subfields == null || subfields.isEmpty()) {
      return true;
    }
    for (MarcSubfield subfield : subfields) {
      if (subfield == null) {
        continue;
      }
      String code = subfield.getSubfield();
      if (code != null && TARGET_999_SUBFIELDS.contains(code)) {
        return true;
      }
    }
    return false;
  }

  private static long resolveCacheExpirationSeconds() {
    String raw = System.getProperty(CACHE_EXPIRATION_TIME_ENV);
    if (StringUtils.isBlank(raw)) {
      raw = System.getenv(CACHE_EXPIRATION_TIME_ENV.replace('.', '_').replace('-', '_').toUpperCase());
    }
    if (StringUtils.isBlank(raw)) {
      raw = DEFAULT_EXPIRATION_TIME_SECONDS;
    }
    try {
      return Long.parseLong(raw);
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid value for {} = '{}', falling back to default {}s",
        CACHE_EXPIRATION_TIME_ENV, raw, DEFAULT_EXPIRATION_TIME_SECONDS);
      return Long.parseLong(DEFAULT_EXPIRATION_TIME_SECONDS);
    }
  }
}
