package org.folio.inventory.eventhandlers;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.actions.UpdateInstanceEventHandler;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle.TITLE_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdateInstanceEventHandlerUnitTest {

  private static final String PARSED_CONTENT_WITH_PRECEDING_SUCCEEDING_TITLES = "{\"leader\": \"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"ybp7406411\"},{\"780\": {\"ind1\":\"0\",\"ind2\":\"0\", \"subfields\":[{\"t\":\"Houston oil directory\"}]}},{ \"785\": { \"ind1\": \"0\", \"ind2\": \"0\", \"subfields\": [ { \"t\": \"SAIS review of international affairs\" }, {\"x\": \"1945-4724\" }]}}]}";
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/rules.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/record.json";
  private static final String INSTANCE_ID = "ddd266ef-07ac-4117-be13-d418b8cd6902";

  @Mock
  private Storage storage;
  @Mock
  private Context context;
  @Mock
  InstanceCollection instanceRecordCollection;
  @Mock
  OkapiHttpClient okapiHttpClient;

  private UpdateInstanceEventHandler updateInstanceEventHandler;
  private InstanceUpdateDelegate instanceUpdateDelegate;
  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private JsonObject mappingRules;
  private JsonObject record;
  private Instance existingInstance;
  private Map<String,String> headers = new HashMap<>();

  @Before
  public void setUp() throws IOException {
    headers.put("x-okapi-url", "localhost");
    headers.put("x-okapi-tenant", "dummy");
    headers.put("x-okapi-token", "token");
    MockitoAnnotations.initMocks(this);
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    instanceUpdateDelegate = Mockito.spy(new InstanceUpdateDelegate(storage));
    precedingSucceedingTitlesHelper = Mockito.spy(new PrecedingSucceedingTitlesHelper(ctxt -> okapiHttpClient));
    updateInstanceEventHandler = new UpdateInstanceEventHandler(instanceUpdateDelegate, context, precedingSucceedingTitlesHelper);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      Instance instanceRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instanceRecord));
      return null;
    }).when(instanceRecordCollection).update(any(), any(Consumer.class), any(Consumer.class));

    when(okapiHttpClient.get(anyString()))
      .thenReturn(CompletableFuture.completedFuture(new Response(200, new JsonObject().encode(), null, null)));
    when(okapiHttpClient.post(any(URL.class), any(JsonObject.class)))
      .thenReturn(CompletableFuture.completedFuture(new Response(201, null, null, null)));

    when(context.getTenantId()).thenReturn("dummy");
    when(context.getToken()).thenReturn("token");
    when(context.getOkapiLocation()).thenReturn("http://localhost");

    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    record = new JsonObject(TestUtil.readFileFromPath(RECORD_PATH));
  }

  @Test
  public void shouldProcessEvent() {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("MARC_BIB", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<Instance> future =
      updateInstanceEventHandler.handle(eventPayload);
    Instance updatedInstance = future.result();

    Assert.assertNotNull(updatedInstance);
    Assert.assertEquals(INSTANCE_ID, updatedInstance.getId());
    Assert.assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    Assert.assertNotNull(updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value)).findFirst().get());
    Assert.assertNotNull(updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst().get());
    Assert.assertNotNull(updatedInstance.getSubjects());
    Assert.assertEquals(1, updatedInstance.getSubjects().size());
    Assert.assertTrue(updatedInstance.getSubjects().get(0).contains("additional subfield"));
    Assert.assertFalse(updatedInstance.getSubjects().get(0).contains("Environmentalism in literature"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);

    ArgumentCaptor<Context> argument = ArgumentCaptor.forClass(Context.class);
    verify(instanceUpdateDelegate).handle(any(), any(), argument.capture());
    Assert.assertEquals("token", argument.getValue().getToken());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("http://localhost", argument.getValue().getOkapiLocation());
  }

  @Test
  public void shouldAddPrecedingAndSucceedingTitlesFromIncomingRecord() throws IOException {
    Record record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.getParsedRecord().withContent(PARSED_CONTENT_WITH_PRECEDING_SUCCEEDING_TITLES);

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("MARC_BIB", Json.encode(record));
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<Instance> future = updateInstanceEventHandler.handle(eventPayload);
    Instance updatedInstance = future.result();

    Assert.assertNotNull(updatedInstance);
    Assert.assertEquals(INSTANCE_ID, updatedInstance.getId());
    Assert.assertTrue(existingInstance.getPrecedingTitles().isEmpty());
    Assert.assertTrue(existingInstance.getSucceedingTitles().isEmpty());
    Assert.assertEquals(1, updatedInstance.getPrecedingTitles().size());
    Assert.assertEquals("Houston oil directory", updatedInstance.getPrecedingTitles().get(0).toPrecedingTitleJson().getString(TITLE_KEY));
    Assert.assertEquals(1, updatedInstance.getSucceedingTitles().size());
    Assert.assertEquals("SAIS review of international affairs", updatedInstance.getSucceedingTitles().get(0).toSucceedingTitleJson().getString(TITLE_KEY));
    verify(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(Instance.class), any(Context.class));
  }

  @Test
  public void shouldCompleteExceptionally() throws IOException {

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("MARC", ZIPArchiver.zip(record.encode()));
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<Instance> future =
      updateInstanceEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
  }

  @Test
  public void shouldSendError() {

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = (Consumer<Failure>) invocationOnMock.getArguments()[2];
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(instanceRecordCollection).update(any(), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("MARC", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<Instance> future =
      updateInstanceEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
  }

}
