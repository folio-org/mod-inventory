package support.fakes.processors;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;

import api.ApiTestSuite;
import api.support.http.StorageInterfaceUrls;
import io.vertx.core.json.JsonObject;

public final class StorageRecordPreProcessors {

  // Holdings record property name, item property name, effective property name
  private static final List<Triple<String, String, String>> CALL_NUMBER_PROPERTIES = Arrays.asList(
    new ImmutableTriple<>("callNumber", "itemLevelCallNumber", "callNumber"),
    new ImmutableTriple<>("callNumberPrefix", "itemLevelCallNumberPrefix", "prefix"),
    new ImmutableTriple<>("callNumberSuffix", "itemLevelCallNumberSuffix", "suffix")
  );

  private static final String HOLDINGS_RECORD_PROPERTY_NAME = "holdingsRecordId";
  private static final String PERMANENT_LOCATION_PROPERTY = "permanentLocationId";
  private static final String TEMPORARY_LOCATION_PROPERTY = "temporaryLocationId";

  private StorageRecordPreProcessors() {
    throw new AssertionError("Do not instantiate");
  }

  public static CompletableFuture<JsonObject> setEffectiveLocationForItem(
    @SuppressWarnings("unused") JsonObject oldItem, JsonObject newItem) {

    CompletableFuture<JsonObject> holdings = completedFuture(new JsonObject());
    if (StringUtils.isBlank(newItem.getString(TEMPORARY_LOCATION_PROPERTY))
      && StringUtils.isBlank(newItem.getString(PERMANENT_LOCATION_PROPERTY))) {
      holdings = getHoldingById(newItem.getString(HOLDINGS_RECORD_PROPERTY_NAME));
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
    @SuppressWarnings("unused") JsonObject oldItem, JsonObject newItem) {

    CompletableFuture<JsonObject> holdings = completedFuture(new JsonObject());
    boolean shouldRetrieveHoldings = CALL_NUMBER_PROPERTIES.stream()
      .map(Triple::getMiddle)
      .anyMatch(property -> StringUtils.isBlank(newItem.getString(property)));

    if (shouldRetrieveHoldings) {
      String holdingsId = newItem.getString(HOLDINGS_RECORD_PROPERTY_NAME);
      holdings = getHoldingById(holdingsId);
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

      return newItem.put("effectiveCallNumberComponents", effectiveCallNumberComponents);
    });
  }

  private static CompletableFuture<JsonObject> getHoldingById(String id) {
    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    try {
      ApiTestSuite.createOkapiHttpClient()
        .get(
          StorageInterfaceUrls.holdingStorageUrl("?query=id=" + id),
          ResponseHandler.json(getCompleted)
        );
    } catch (MalformedURLException ex) {
      getCompleted.completeExceptionally(ex);
    }

    return getCompleted.thenApply(
      response -> response.getJson().getJsonArray("holdingsRecords")
        .getJsonObject(0)
    );
  }
}
