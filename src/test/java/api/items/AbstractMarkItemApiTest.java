package api.items;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static support.fixtures.InstanceFixture.smallAngryPlanet;
import static support.matchers.RequestMatchers.hasStatus;
import static support.matchers.RequestMatchers.isOpenNotYetFilled;
import static support.matchers.ResponseMatchers.hasValidationError;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import support.ApiTests;
import support.builders.HoldingRequestBuilder;
import support.builders.ItemRequestBuilder;
import support.dto.Request;

public abstract class AbstractMarkItemApiTest extends ApiTests {

  protected IndividualResource holdingsRecord;

  @BeforeEach
  void createInstanceAndHoldingsRecord() {
    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    holdingsRecord = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance.getId()));
  }

  @Test
  void canMarkItemWhenInAllowedStatus() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(allowedSourceStatus())
      .canCirculate());
    final Response response = markItem(createdItem);

    assertEquals(200, response.statusCode());
    assertThat(response.getJson(), targetStatusMatcher());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), targetStatusMatcher());
  }

  @Test
  void cannotMarkItemWhenNotInAllowedStatus() {
    final String initialStatus = disallowedSourceStatus();
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(initialStatus)
      .canCirculate());

    assertThat(markItem(createdItem), hasValidationError(
      "Item is not allowed to be marked as " + targetStatusName(), "status.name", initialStatus));
  }

  @Test
  void shouldNotMarkItemThatCannotBeFound() {
    assertThat(markItem(UUID.randomUUID()).statusCode(),
      is(404));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  void shouldChangeRequestBeingFulfilledBackToNotYetFilled(String requestStatus) {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(requestStatus.replace("Open - ", ""))
      .canCirculate());

    final IndividualResource request = createRequest(createdItem.getId(),
      requestStatus, DateTime.now(DateTimeZone.UTC).plusHours(1));

    assertThat(markItem(createdItem).getJson(), targetStatusMatcher());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), targetStatusMatcher());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      isOpenNotYetFilled());
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  void shouldNotReopenExpiredRequests(String requestStatus) {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(requestStatus.replace("Open - ", ""))
      .canCirculate());

    final IndividualResource request = createRequest(createdItem.getId(),
      requestStatus, DateTime.now(DateTimeZone.UTC).minusHours(1));

    assertThat(markItem(createdItem).getJson(), targetStatusMatcher());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), targetStatusMatcher());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      hasStatus(requestStatus));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "Closed - Cancelled",
    "Closed - Filled",
    "Closed - Pickup expired",
    "Closed - Unfilled"
  })
  void shouldNotReopenClosedRequests(String requestStatus) {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Awaiting pickup")
      .canCirculate());

    final IndividualResource request = createRequest(createdItem.getId(),
      requestStatus, DateTime.now(DateTimeZone.UTC).plusHours(1));

    assertThat(markItem(createdItem).getJson(), targetStatusMatcher());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), targetStatusMatcher());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      hasStatus(requestStatus));
  }

  private IndividualResource createRequest(UUID itemId, String status, DateTime expireDateTime) {
    return requestStorageClient.create(JsonObject.mapFrom(Request.builder()
      .status(status)
      .itemId(itemId.toString())
      .holdShelfExpirationDate(expireDateTime.toDate())
      .requesterId(UUID.randomUUID().toString())
      .requestType("Hold")
      .build()));
  }

  protected abstract Response markItem(IndividualResource item);

  protected abstract Response markItem(UUID id);

  protected abstract Matcher<JsonObject> targetStatusMatcher();

  protected abstract String targetStatusName();

  protected abstract String allowedSourceStatus();

  protected abstract String disallowedSourceStatus();
}
