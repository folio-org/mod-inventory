package api.items;

import static api.support.InstanceSamples.smallAngryPlanet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static support.matchers.ItemMatchers.isMissing;
import static support.matchers.ItemMatchers.isWithdrawn;
import static support.matchers.RequestMatchers.hasStatus;
import static support.matchers.RequestMatchers.isOpenNotYetFilled;
import static support.matchers.ResponseMatchers.hasValidationError;

import java.util.UUID;

import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.dto.Request;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import lombok.SneakyThrows;

@RunWith(JUnitParamsRunner.class)
public class MarkItemWithdrawnApiTests extends ApiTests {
  private IndividualResource holdingsRecord;

  @Before
  public void createInstanceAndHoldingsRecord() throws Exception {
    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    holdingsRecord = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance.getId()));
  }

  @Parameters({
    "Paged"
  })
  @Test
  public void canWithdrawItemWhenInAllowedStatus(String initialStatus) throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(initialStatus)
      .canCirculate());

    assertThat(markItemWithdrawn(createdItem).getJson(), isWithdrawn());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isWithdrawn());
  }

  @Parameters({
    "Checked out"
  })
  @Test
  public void cannotWithdrawIItemWhenNotInAllowedStatus(String initialStatus) throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(initialStatus)
      .canCirculate());

    assertThat(markItemWithdrawn(createdItem), hasValidationError(
      "Item is not allowed to be marked as Withdrawn", "status.name", initialStatus));
  }

  @Test
  public void shouldWithdrawItemThatCannotBeFound() {
    assertThat(markWithdrawnFixture.markWithdrawn(UUID.randomUUID()).getStatusCode(),
      is(404));
  }

  @Test
  @Parameters({
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  public void shouldChangeRequestBeingFulfilledBackToNotYetFilled(String requestStatus) throws Exception {
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

  @Test
  @Parameters({
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  public void shouldNotReopenExpiredRequests(String requestStatus) throws Exception {
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

  @Test
  @Parameters({
    "Closed - Cancelled",
    "Closed - Filled",
    "Closed - Pickup expired",
    "Closed - Unfilled"
  })
  public void shouldNotReopenClosedRequests(String requestStatus) throws Exception {
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
  public void canMarkWithdrawnItemAsMissing() throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Available")
      .canCirculate());

    markItemWithdrawn(createdItem);
    markMissingFixture.markMissing(createdItem);

    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isMissing());
  }

  private Response markItemWithdrawn(IndividualResource item) {
    return markWithdrawnFixture.markWithdrawn(item);
  }

  @SneakyThrows
  private IndividualResource createRequest(UUID itemId, String status, DateTime expiryDateTime) {
    return requestStorageClient.create(Request.builder()
      .status(status)
      .itemId(itemId.toString())
      .holdShelfExpirationDate(expiryDateTime.toDate())
      .requesterId(UUID.randomUUID().toString())
      .requestType("Hold")
      .build());
  }
}
