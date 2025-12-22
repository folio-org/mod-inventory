package org.folio.inventory.eventhandlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.folio.HoldingsType;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import org.folio.HoldingsRecord;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.handlers.actions.HoldingsUpdateDelegate;
import org.folio.inventory.dataimport.handlers.quickmarc.UpdateHoldingsQuickMarcEventHandler;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;

@RunWith(MockitoJUnitRunner.class)
public class UpdateHoldingsQuickMarcEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/holdings-rules.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/holdings.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/holdings-record.json";
  private static final String HOLDINGS_WITH_CELL_NUMBER_PATH = "src/test/resources/handlers/holdings-with-call-number.json";
  private static final String HOLDINGS_RECORD_WITHOUT_CELL_NUMBER_PATH = "src/test/resources/handlers/holdings-record-without-call-number.json";
  private static final String HOLDINGS_ID = "65cb2bf0-d4c2-4886-8ad0-b76f1ba75d61";
  private static final String HOLDINGS_TYPE_ID = "fe19bae4-da28-472b-be90-d442e2428eadx";

  @Mock
  private Storage storage;
  @Mock
  private HoldingsCollectionService holdingsCollectionService;
  @Mock
  private HoldingsRecordsSourceCollection sourceCollection;
  @Mock
  private Context context;
  @Mock
  private HoldingsRecordCollection holdingsRecordCollection;

  private UpdateHoldingsQuickMarcEventHandler updateHoldingsQuickMarcEventHandler;
  private HoldingsUpdateDelegate holdingsUpdateDelegate;
  private JsonObject mappingRules;
  private JsonObject holdingsRecordJson;
  private HoldingsRecord existingHoldingsRecord;
  private HoldingsRecord existingHoldingsRecordWithCallNumber;

  @Before
  public void setUp() throws IOException {
    existingHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)).mapTo(HoldingsRecord.class);
    existingHoldingsRecordWithCallNumber = new JsonObject(TestUtil.readFileFromPath(
      HOLDINGS_WITH_CELL_NUMBER_PATH)).mapTo(HoldingsRecord.class);
    holdingsUpdateDelegate = Mockito.spy(new HoldingsUpdateDelegate(storage, holdingsCollectionService));
    updateHoldingsQuickMarcEventHandler = new UpdateHoldingsQuickMarcEventHandler(holdingsUpdateDelegate, context);

    var sourceId = String.valueOf(UUID.randomUUID());
    when(storage.getHoldingsRecordCollection(any(Context.class))).thenReturn(holdingsRecordCollection);
    when(storage.getHoldingsRecordsSourceCollection(any(Context.class))).thenReturn(sourceCollection);
    when(holdingsCollectionService.findSourceIdByName(any(HoldingsRecordsSourceCollection.class), any())).thenReturn(Future.succeededFuture(sourceId));

    doAnswer(invocationOnMock -> {
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingHoldingsRecord));
      return null;
    }).when(holdingsRecordCollection).findById(eq("ddd266ef-07ac-4117-be13-d418b8cd6902"), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingHoldingsRecordWithCallNumber));
      return null;
    }).when(holdingsRecordCollection).findById(eq("abc266ef-07ac-4117-be13-d418b8cd6902"), any(), any());

    doAnswer(invocationOnMock -> {
      HoldingsRecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordCollection).update(any(), any(), any());

    when(context.getTenantId()).thenReturn("dummy");
    when(context.getToken()).thenReturn("token");
    when(context.getOkapiLocation()).thenReturn("http://localhost");

    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    holdingsRecordJson = new JsonObject(TestUtil.readFileFromPath(RECORD_PATH));
  }

  @Test
  public void shouldProcessEvent() {
    List<HoldingsType> holdings = new ArrayList<>();
    holdings.add(new HoldingsType().withName("testingnote$a").withId(HOLDINGS_TYPE_ID));
    MappingParameters mappingParameters = new MappingParameters();
    mappingParameters.withHoldingsTypes(holdings);

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_HOLDING");
    eventPayload.put("MARC_HOLDING", holdingsRecordJson.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", Json.encode(mappingParameters));

    Future<HoldingsRecord> future = updateHoldingsQuickMarcEventHandler.handle(eventPayload);
    HoldingsRecord updatedHoldings = future.result();

    Assert.assertNotNull(updatedHoldings);
    Assert.assertEquals(HOLDINGS_ID, updatedHoldings.getId());

    Assert.assertNull(updatedHoldings.getHoldingsTypeId());
    Assert.assertNotNull(updatedHoldings.getHoldingsStatements());
    Assert.assertEquals(21, updatedHoldings.getHoldingsStatements().size());
    Assert.assertNotNull(updatedHoldings.getNotes());
    Assert.assertEquals("testingnote$a ; testingnote$u ; testingnote$3 ; testingnote$5 ; testingnote$6 ; testingnote$8",
      updatedHoldings.getNotes().getFirst().getNote());

    ArgumentCaptor<Context> argument = ArgumentCaptor.forClass(Context.class);
    verify(holdingsUpdateDelegate).handle(any(), any(), argument.capture());
    Assert.assertEquals("token", argument.getValue().getToken());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("http://localhost", argument.getValue().getOkapiLocation());
  }

  @Test
  public void shouldProcessCallNumberRemoveEvent() throws IOException {
    holdingsRecordJson = new JsonObject(TestUtil.readFileFromPath(HOLDINGS_RECORD_WITHOUT_CELL_NUMBER_PATH));
    List<HoldingsType> holdings = new ArrayList<>();
    holdings.add(new HoldingsType().withName("testingnote$a").withId(HOLDINGS_TYPE_ID));
    MappingParameters mappingParameters = new MappingParameters();
    mappingParameters.withHoldingsTypes(holdings);

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_HOLDING");
    eventPayload.put("MARC_HOLDING", holdingsRecordJson.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", Json.encode(mappingParameters));

    Future<HoldingsRecord> future = updateHoldingsQuickMarcEventHandler.handle(eventPayload);
    HoldingsRecord updatedHoldings = future.result();

    Assert.assertNotNull(updatedHoldings);
    Assert.assertEquals(HOLDINGS_ID, updatedHoldings.getId());

    Assert.assertNull(updatedHoldings.getHoldingsTypeId());
    Assert.assertNull(updatedHoldings.getCallNumber());
    Assert.assertNull(updatedHoldings.getCallNumberPrefix());
    Assert.assertNull(updatedHoldings.getCallNumberSuffix());
    Assert.assertNull(updatedHoldings.getCallNumberTypeId());
    Assert.assertNull(updatedHoldings.getCopyNumber());
    Assert.assertNull(updatedHoldings.getShelvingTitle());

    ArgumentCaptor<Context> argument = ArgumentCaptor.forClass(Context.class);
    verify(holdingsUpdateDelegate).handle(any(), any(), argument.capture());
    Assert.assertEquals("token", argument.getValue().getToken());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("http://localhost", argument.getValue().getOkapiLocation());
  }

  @Test
  public void shouldCompleteExceptionally_whenRecordIsEmpty() {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_HOLDING");
    eventPayload.put("MARC_HOLDING", "");
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<HoldingsRecord> future = updateHoldingsQuickMarcEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
  }

  @Test
  public void shouldSendError() {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(holdingsRecordCollection).update(any(), any(), any());


    List<HoldingsType> holdings = new ArrayList<>();
    holdings.add(new HoldingsType().withName("testingnote$a").withId(HOLDINGS_TYPE_ID));
    MappingParameters mappingParameters = new MappingParameters();
    mappingParameters.withHoldingsTypes(holdings);

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_HOLDING");
    eventPayload.put("MARC_HOLDING", holdingsRecordJson.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", Json.encode(mappingParameters));

    Future<HoldingsRecord> future = updateHoldingsQuickMarcEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
  }
}
