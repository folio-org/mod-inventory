package support.fakes.processors;

import api.ApiTestSuite;
import api.support.http.StorageInterfaceUrls;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;

public final class StorageRecordPreProcessors {
  private static final AtomicLong hridSequence = new AtomicLong(1L);

  // Holdings record property name, item property name, effective property name
  private static final List<Triple<String, String, String>> CALL_NUMBER_PROPERTIES = Arrays.asList(
    new ImmutableTriple<>("callNumber", "itemLevelCallNumber", "callNumber"),
    new ImmutableTriple<>("callNumberPrefix", "itemLevelCallNumberPrefix", "prefix"),
    new ImmutableTriple<>("callNumberSuffix", "itemLevelCallNumberSuffix", "suffix"),
    new ImmutableTriple<>("callNumberTypeId", "itemLevelCallNumberTypeId", "typeId")
  );

  private static final String HOLDINGS_RECORD_PROPERTY_NAME = "holdingsRecordId";
  private static final String PERMANENT_LOCATION_PROPERTY = "permanentLocationId";
  private static final String TEMPORARY_LOCATION_PROPERTY = "temporaryLocationId";
  private static final String EFFECTIVE_CALL_NUMBER_COMPONENTS = "effectiveCallNumberComponents";
  // RMB uses ISO-8601 compatible date time format by default.
  private static final String RMB_DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS+0000";

  private StorageRecordPreProcessors() {
    throw new AssertionError("Do not instantiate");
  }

  public static CompletableFuture<JsonObject> setEffectiveLocationForItem(
    @SuppressWarnings("unused") String tenant, JsonObject oldItem, JsonObject newItem) {

    CompletableFuture<JsonObject> holdings = completedFuture(new JsonObject());
    if (StringUtils.isBlank(newItem.getString(TEMPORARY_LOCATION_PROPERTY))
      && StringUtils.isBlank(newItem.getString(PERMANENT_LOCATION_PROPERTY))) {

      holdings = findHoldingForItem(tenant, newItem);
    }

    return holdings.thenApply(holdingsRecord ->
      newItem.put("effectiveLocationId", ObjectUtils.firstNonNull(
        newItem.getString(TEMPORARY_LOCATION_PROPERTY),
        newItem.getString(PERMANENT_LOCATION_PROPERTY),
        holdingsRecord.getString(TEMPORARY_LOCATION_PROPERTY),
        holdingsRecord.getString(PERMANENT_LOCATION_PROPERTY)
      ))
    );
  }

  public static CompletableFuture<JsonObject> setEffectiveCallNumberComponents(
    @SuppressWarnings("unused") String tenant, JsonObject oldItem, JsonObject newItem) {

    CompletableFuture<JsonObject> holdings = completedFuture(new JsonObject());
    boolean shouldRetrieveHoldings = CALL_NUMBER_PROPERTIES.stream()
      .map(Triple::getMiddle)
      .anyMatch(property -> StringUtils.isBlank(newItem.getString(property)));

    if (shouldRetrieveHoldings) {
      holdings = findHoldingForItem(tenant, newItem);
    }

    return holdings.thenApply(holding -> {
      JsonObject effectiveCallNumberComponents = new JsonObject();

      CALL_NUMBER_PROPERTIES.forEach(properties -> {
        String itemPropertyName = properties.getMiddle();
        String holdingsPropertyName = properties.getLeft();
        String effectivePropertyName = properties.getRight();

        final String propertyValue = ObjectUtils.firstNonNull(
          newItem.getString(itemPropertyName),
          holding.getString(holdingsPropertyName)
        );

        effectiveCallNumberComponents.put(effectivePropertyName, propertyValue);
      });

      return newItem.put(EFFECTIVE_CALL_NUMBER_COMPONENTS, effectiveCallNumberComponents);
    });
  }

  public static CompletableFuture<JsonObject> setEffectiveShelvingOrder(
    @SuppressWarnings("unused") String tenant, JsonObject oldItem, JsonObject newItem) {

    final var effectiveCallNumberComponents
      = newItem.getJsonObject(EFFECTIVE_CALL_NUMBER_COMPONENTS);

    return completedFuture(newItem.put("effectiveShelvingOrder",
      effectiveCallNumberComponents.getString("callNumber")));
  }

  public static CompletableFuture<JsonObject> setStatusDateProcessor(
    String tenant, JsonObject oldItem, JsonObject newItem) {

    // Create case
    if (oldItem == null) {
      return completedFuture(newItem);
    }

    return findHoldingForItem(tenant, newItem).thenApply(holding -> {
      final String oldStatus = getNestedProperty(oldItem, "status", "name");
      final String newStatus = getNestedProperty(newItem, "status", "name");

      if (!Objects.equals(oldStatus, newStatus)) {
        JsonObject newStatusObject = newItem.containsKey("status")
          ? newItem.getJsonObject("status")
          : new JsonObject();

        newStatusObject = newStatusObject.put("date",
          DateTime.now(DateTimeZone.UTC)
            .toString(RMB_DATETIME_PATTERN)
        );
        return newItem.put("status", newStatusObject);
      }

      return newItem;
    });
  }

  private static CompletableFuture<JsonObject> getHoldingById(String tenant, String id) {
    if (StringUtils.isBlank(id)) {
      return completedFuture(new JsonObject());
    }

    try {
      return ApiTestSuite.createOkapiHttpClient(tenant).get(
        StorageInterfaceUrls.holdingStorageUrl("?query=id=" + id))
        .thenApply(
          response -> response.getJson().getJsonArray("holdingsRecords").getJsonObject(0))
        .toCompletableFuture();
    } catch (MalformedURLException ex) {
      return CompletableFuture.failedFuture(ex);
    }
  }

  public static RecordPreProcessor setHridProcessor(
    String hridPrefix) {

    return (tenant, oldEntity, newEntity) -> {
      if (StringUtils.isBlank(newEntity.getString("hrid"))) {
        String hridToSet = hridPrefix + hridSequence.getAndIncrement();

        newEntity.put("hrid", hridToSet);
      }

      return completedFuture(newEntity);
    };
  }

  private static CompletableFuture<JsonObject> findHoldingForItem(String tenant, JsonObject item) {
    final String holdingsId = item.getString(HOLDINGS_RECORD_PROPERTY_NAME);

    return getHoldingById(tenant, holdingsId);
  }
}
