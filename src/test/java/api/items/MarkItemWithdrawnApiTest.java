package api.items;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static support.fixtures.InstanceFixture.smallAngryPlanet;
import static support.matchers.ItemMatchers.isMissing;
import static support.matchers.ItemMatchers.isWithdrawn;
import static support.matchers.RequestMatchers.hasStatus;
import static support.matchers.RequestMatchers.isOpenNotYetFilled;
import static support.matchers.ResponseMatchers.hasValidationError;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import lombok.SneakyThrows;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
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

public class MarkItemWithdrawnApiTest extends ApiTests {

  private IndividualResource holdingsRecord;

  @BeforeEach
  void createInstanceAndHoldingsRecord() {
    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    holdingsRecord = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance.getId()));
  }

  @Test
  void canWithdrawItemWhenInAllowedStatus() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Paged")
      .canCirculate());

    assertThat(markItemWithdrawn(createdItem).getJson(), isWithdrawn());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isWithdrawn());
  }

  @Test
  void cannotWithdrawIItemWhenNotInAllowedStatus() {
    final String initialStatus = "Checked out";
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(initialStatus)
      .canCirculate());

    assertThat(markItemWithdrawn(createdItem), hasValidationError(
      "Item is not allowed to be marked as Withdrawn", "status.name", initialStatus));
  }

  @Test
  void shouldWithdrawItemThatCannotBeFound() {
    assertThat(markItemFixture.markWithdrawn(UUID.randomUUID()).statusCode(),
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

    assertThat(markItemWithdrawn(createdItem).getJson(), isWithdrawn());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isWithdrawn());

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

    assertThat(markItemWithdrawn(createdItem).getJson(), isWithdrawn());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isWithdrawn());

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

    assertThat(markItemWithdrawn(createdItem).getJson(), isWithdrawn());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isWithdrawn());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      hasStatus(requestStatus));
  }

  @Test
  void canMarkWithdrawnItemAsMissing() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Available")
      .canCirculate());

    markItemWithdrawn(createdItem);
    markItemFixture.markMissing(createdItem.getId());

    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isMissing());
  }

  private Response markItemWithdrawn(IndividualResource item) {
    return markItemFixture.markWithdrawn(item.getId());
  }

  @SneakyThrows
  private IndividualResource createRequest(UUID itemId, String status, DateTime expiryDateTime) {
    return requestStorageClient.create(JsonObject.mapFrom(Request.builder()
      .status(status)
      .itemId(itemId.toString())
      .holdShelfExpirationDate(expiryDateTime.toDate())
      .requesterId(UUID.randomUUID().toString())
      .requestType("Hold")
      .build()));
  }
}
