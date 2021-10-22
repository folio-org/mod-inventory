package org.folio.inventory.eventhandlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.function.Consumer;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
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
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.processing.events.utils.ZIPArchiver;

@RunWith(MockitoJUnitRunner.class)
public class UpdateHoldingsQuickMarcEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/holdings-rules.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/holdings.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/holdings-record.json";
  private static final String HOLDINGS_ID = "65cb2bf0-d4c2-4886-8ad0-b76f1ba75d61";
  private static final Integer HOLDINGS_VERSION = 1;

  @Mock
  private Storage storage;
  @Mock
  private Context context;
  @Mock
  private HoldingsRecordCollection holdingsRecordCollection;
  @Mock
  private OkapiHttpClient okapiHttpClient;

  private UpdateHoldingsQuickMarcEventHandler updateHoldingsQuickMarcEventHandler;
  private HoldingsUpdateDelegate holdingsUpdateDelegate;
  private JsonObject mappingRules;
  private JsonObject record;
  private HoldingsRecord existingHoldingsRecord;

  @Before
  public void setUp() throws IOException {
    existingHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)).mapTo(HoldingsRecord.class);
    holdingsUpdateDelegate = Mockito.spy(new HoldingsUpdateDelegate(storage));
    updateHoldingsQuickMarcEventHandler = new UpdateHoldingsQuickMarcEventHandler(holdingsUpdateDelegate, context);

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordCollection);
    doAnswer(invocationOnMock -> {
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingHoldingsRecord));
      return null;
    }).when(holdingsRecordCollection).findById(anyString(), any(), any());

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
    record = new JsonObject(TestUtil.readFileFromPath(RECORD_PATH));
  }

  @Test
  public void shouldProcessEvent() {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_HOLDING");
    eventPayload.put("MARC_HOLDING", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());
    eventPayload.put("RELATED_RECORD_VERSION", HOLDINGS_VERSION.toString());

    Future<HoldingsRecord> future = updateHoldingsQuickMarcEventHandler.handle(eventPayload);
    HoldingsRecord updatedHoldings = future.result();

    Assert.assertNotNull(updatedHoldings);
    Assert.assertEquals(HOLDINGS_ID, updatedHoldings.getId());
    Assert.assertEquals(HOLDINGS_VERSION, updatedHoldings.getVersion());

    Assert.assertEquals("fe19bae4-da28-472b-be90-d442e2428ead", updatedHoldings.getHoldingsTypeId());
    Assert.assertNotNull(updatedHoldings.getHoldingsStatements());
    Assert.assertEquals(21, updatedHoldings.getHoldingsStatements().size());
    Assert.assertNotNull(updatedHoldings.getNotes());
    Assert.assertEquals("note$a ; note$u ; note$3 ; note$5 ; note$6 ; note$8", updatedHoldings.getNotes().get(0).getNote());

    ArgumentCaptor<Context> argument = ArgumentCaptor.forClass(Context.class);
    verify(holdingsUpdateDelegate).handle(any(), any(), argument.capture());
    Assert.assertEquals("token", argument.getValue().getToken());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("http://localhost", argument.getValue().getOkapiLocation());
  }

  @Test
  public void shouldCompleteExceptionally() throws IOException {

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_HOLDING");
    eventPayload.put("MARC_HOLDING", ZIPArchiver.zip(record.encode()));
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

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_HOLDING");
    eventPayload.put("MARC_HOLDING", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<HoldingsRecord> future = updateHoldingsQuickMarcEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
  }

}
