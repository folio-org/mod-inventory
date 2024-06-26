package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.Record;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.ReactToType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_MARC_FOR_UPDATE_RECEIVED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.EntityType.AUTHORITY;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class CommonMatchEventHandlerTest {

  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "token";
  private static final String OKAPI_URL = "localhost";
  private static final String INSTANCES_IDS_KEY = "INSTANCES_IDS";

  @Mock
  private EventHandler matchMarcBibHandler;
  @Mock
  private EventHandler matchInstanceHandler;
  @Mock
  private EventHandler matchHoldingsHandler;
  @Mock
  private EventHandler matchItemHandler;
  private AutoCloseable closeable;
  private EventHandler eventHandler;

  @Before
  public void setUp() {
    this.closeable = MockitoAnnotations.openMocks(this);
    eventHandler = new CommonMatchEventHandler(
      List.of(matchMarcBibHandler, matchInstanceHandler, matchHoldingsHandler, matchItemHandler));
  }

  @After
  public void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void shouldCallMatchInstanceEventHandlerIfCurrentNodeIsMatchInstanceProfile() throws ExecutionException, InterruptedException, TimeoutException {
    MatchProfile instanceMatchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE);

    ProfileSnapshotWrapper instanceMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withReactTo(ReactToType.MATCH)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(instanceMatchProfile).getMap());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(instanceMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
      }});

    when(matchInstanceHandler.handle(eventPayload)).thenReturn(CompletableFuture.completedFuture(eventPayload));
    when(matchInstanceHandler.isEligible(argThat(payload -> payload.getCurrentNode().equals(instanceMatchProfileWrapper))))
      .thenReturn(true);

    eventHandler.handle(eventPayload).get(5, TimeUnit.SECONDS);

    verify(matchInstanceHandler).handle(eventPayload);
  }

  @Test
  public void shouldCallMatchHoldingsEventHandlerIfCurrentNodeIsMatchHoldingsProfile() throws ExecutionException, InterruptedException, TimeoutException {
    MatchProfile instanceMatchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS);

    ProfileSnapshotWrapper instanceMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withReactTo(ReactToType.MATCH)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(instanceMatchProfile).getMap());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(instanceMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
      }});

    when(matchHoldingsHandler.handle(eventPayload)).thenReturn(CompletableFuture.completedFuture(eventPayload));
    when(matchHoldingsHandler.isEligible(argThat(payload -> payload.getCurrentNode().equals(instanceMatchProfileWrapper))))
      .thenReturn(true);

    eventHandler.handle(eventPayload).get(5, TimeUnit.SECONDS);

    verify(matchHoldingsHandler).handle(eventPayload);
  }

  @Test
  public void shouldCallMatchItemEventHandlerIfCurrentNodeIsMatchItemProfile() throws ExecutionException, InterruptedException, TimeoutException {
    MatchProfile instanceMatchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ITEM);

    ProfileSnapshotWrapper instanceMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withReactTo(ReactToType.MATCH)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(instanceMatchProfile).getMap());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(instanceMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
      }});

    when(matchItemHandler.handle(eventPayload)).thenReturn(CompletableFuture.completedFuture(eventPayload));
    when(matchItemHandler.isEligible(argThat(payload -> payload.getCurrentNode().equals(instanceMatchProfileWrapper))))
      .thenReturn(true);

    eventHandler.handle(eventPayload).get(5, TimeUnit.SECONDS);

    verify(matchItemHandler).handle(eventPayload);
  }

  @Test
  public void shouldCallMatchMarcBibEventHandlerIfCurrentNodeIsMatchMarcBibProfile()
    throws ExecutionException, InterruptedException, TimeoutException {
    MatchProfile instanceMatchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(MARC_BIBLIOGRAPHIC);

    ProfileSnapshotWrapper instanceMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withReactTo(ReactToType.MATCH)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(instanceMatchProfile).getMap());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(instanceMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
      }});

    when(matchMarcBibHandler.handle(eventPayload)).thenReturn(CompletableFuture.completedFuture(eventPayload));
    when(matchMarcBibHandler.isEligible(argThat(payload -> payload.getCurrentNode().equals(instanceMatchProfileWrapper))))
      .thenReturn(true);

    eventHandler.handle(eventPayload).get(5, TimeUnit.SECONDS);

    verify(matchMarcBibHandler).handle(eventPayload);
  }

  @Test
  public void shouldCallMatchInstanceHandlerIfMultipleMarcBibMatchResultOccursAndNextNodeIsMatchInstanceProfile() throws ExecutionException, InterruptedException, TimeoutException {
    MatchProfile marcBibMatchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(MARC_BIBLIOGRAPHIC);

    MatchProfile instanceMatchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE);

    ProfileSnapshotWrapper instanceMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withReactTo(ReactToType.MATCH)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(instanceMatchProfile).getMap());

    ProfileSnapshotWrapper marcBibMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(marcBibMatchProfile).getMap())
      .withChildSnapshotWrappers(List.of(instanceMatchProfileWrapper));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(marcBibMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
      }});

    DataImportEventPayload marcBibMatchingResultPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(marcBibMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
        put(INSTANCES_IDS_KEY, JsonArray.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()).encode());
      }});

    assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), marcBibMatchingResultPayload.getEventType());
    assertNotNull(marcBibMatchingResultPayload.getContext().get(INSTANCES_IDS_KEY));

    when(matchMarcBibHandler.handle(eventPayload))
      .thenReturn(CompletableFuture.completedFuture(marcBibMatchingResultPayload));
    when(matchInstanceHandler.handle(marcBibMatchingResultPayload))
      .thenReturn(CompletableFuture.completedFuture(marcBibMatchingResultPayload));
    when(matchMarcBibHandler.isEligible(argThat(payload -> payload.getCurrentNode().equals(marcBibMatchProfileWrapper))))
      .thenReturn(true);
    when(matchInstanceHandler.isEligible(argThat(payload -> payload.getCurrentNode().equals(instanceMatchProfileWrapper))))
      .thenReturn(true);

    DataImportEventPayload payload = eventHandler.handle(eventPayload).get(5, TimeUnit.SECONDS);

    verify(matchInstanceHandler).handle(marcBibMatchingResultPayload);
    assertFalse(payload.getContext().containsKey(INSTANCES_IDS_KEY));
  }

  @Test
  public void shouldCallMatchHoldingHandlerIfMultipleMarcBibMatchResultOccursAndNextNodeIsMatchHoldingProfile() throws ExecutionException, InterruptedException, TimeoutException {
    MatchProfile marcBibMatchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(MARC_BIBLIOGRAPHIC);

    MatchProfile holdingsMatchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS);

    ProfileSnapshotWrapper holdingsMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withReactTo(ReactToType.MATCH)
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(holdingsMatchProfile).getMap());

    ProfileSnapshotWrapper marcBibMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(marcBibMatchProfile).getMap())
      .withChildSnapshotWrappers(List.of(holdingsMatchProfileWrapper));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(marcBibMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
      }});

    DataImportEventPayload marcBibMatchingResultPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(marcBibMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
        put(INSTANCES_IDS_KEY, JsonArray.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()).encode());
      }});

    assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), marcBibMatchingResultPayload.getEventType());
    assertNotNull(marcBibMatchingResultPayload.getContext().get(INSTANCES_IDS_KEY));

    when(matchMarcBibHandler.handle(eventPayload))
      .thenReturn(CompletableFuture.completedFuture(marcBibMatchingResultPayload));
    when(matchHoldingsHandler.handle(marcBibMatchingResultPayload))
      .thenReturn(CompletableFuture.completedFuture(marcBibMatchingResultPayload));
    when(matchMarcBibHandler.isEligible(argThat(payload -> payload.getCurrentNode().equals(marcBibMatchProfileWrapper))))
      .thenReturn(true);
    when(matchHoldingsHandler.isEligible(argThat(payload -> payload.getCurrentNode().equals(holdingsMatchProfileWrapper))))
      .thenReturn(true);

    DataImportEventPayload payload = eventHandler.handle(eventPayload).get(5, TimeUnit.SECONDS);

    verify(matchHoldingsHandler).handle(marcBibMatchingResultPayload);
    assertFalse(payload.getContext().containsKey(INSTANCES_IDS_KEY));
  }

  @Test
  public void shouldReturnFailedFutureIfMultipleMarcBibMatchResultOccursAndNextNodeIsNotMatchProfile() {
    MatchProfile marcBibMatchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(MARC_BIBLIOGRAPHIC);

    ProfileSnapshotWrapper marcBibMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(marcBibMatchProfile).getMap())
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile()).getMap())));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(marcBibMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
      }});

    DataImportEventPayload marcBibMatchingResultPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(marcBibMatchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
        put(INSTANCES_IDS_KEY, JsonArray.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()).encode());
      }});

    assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), marcBibMatchingResultPayload.getEventType());
    assertNotNull(marcBibMatchingResultPayload.getContext().get(INSTANCES_IDS_KEY));

    when(matchMarcBibHandler.handle(eventPayload))
      .thenReturn(CompletableFuture.completedFuture(marcBibMatchingResultPayload));
    when(matchMarcBibHandler.isEligible(argThat(payload -> payload.getCurrentNode().equals(marcBibMatchProfileWrapper))))
      .thenReturn(true);

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(eventPayload);

    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentNodeIsNotEligibleMatchProfile() {
    MatchProfile matchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_AUTHORITY)
      .withExistingRecordType(AUTHORITY);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_MARC_FOR_UPDATE_RECEIVED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(matchProfile).getMap()))
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
      }});

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(eventPayload);

    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void shouldReturnFailedFutureIfDedicatedMatchEventHandlerThrowsException() {
    MatchProfile matchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(matchProfile).getMap()))
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
      }});

    when(matchInstanceHandler.isEligible(eventPayload)).thenReturn(true);
    when(matchInstanceHandler.handle(eventPayload)).thenThrow(EventProcessingException.class);

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(eventPayload);

    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  @Parameters({"INSTANCE", "HOLDINGS", "ITEM", "MARC_BIBLIOGRAPHIC"})
  public void shouldReturnTrueIfHandlerIsEligibleForEventPayload(EntityType existingRecordType) {
    MatchProfile matchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(existingRecordType);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withProfileId(matchProfile.getId())
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(matchProfile).getMap()));

    assertTrue(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseIfHandlerIsNotEligibleForPayload() {
    MatchProfile matchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_AUTHORITY)
      .withExistingRecordType(AUTHORITY);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(matchProfile).getMap()));

    assertFalse(eventHandler.isEligible(eventPayload));
  }

}
