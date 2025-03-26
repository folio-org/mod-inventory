package org.folio.inventory.eventhandlers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle.TITLE_KEY;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.quickmarc.UpdateInstanceQuickMarcEventHandler;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.rest.jaxrs.model.Record;

@RunWith(MockitoJUnitRunner.class)
public class UpdateInstanceQuickMarcEventHandlerTest {

  private static final String PARSED_CONTENT_WITH_PRECEDING_SUCCEEDING_TITLES =
    "{\"leader\": \"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"ybp7406411\"},{\"780\": {\"ind1\":\"0\",\"ind2\":\"0\", \"subfields\":[{\"t\":\"Houston oil directory\"}]}},{ \"785\": { \"ind1\": \"0\", \"ind2\": \"0\", \"subfields\": [ { \"t\": \"SAIS review of international affairs\" }, {\"x\": \"1945-4724\" }]}}]}";
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String DELETED_RECORD_PATH = "src/test/resources/handlers/deleted-bib-record.json";
  private static final String INSTANCE_ID = "ddd266ef-07ac-4117-be13-d418b8cd6902";
  private static final String INSTANCE_VERSION = "1";

  @Mock
  private Storage storage;
  @Mock
  private Context context;
  @Mock
  private InstanceCollection instanceRecordCollection;
  @Mock
  private OkapiHttpClient okapiHttpClient;

  private UpdateInstanceQuickMarcEventHandler updateInstanceEventHandler;
  private InstanceUpdateDelegate instanceUpdateDelegate;
  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private JsonObject mappingRules;
  private JsonObject record;
  private JsonObject deletedRecord;
  private Instance existingInstance;
  private Instance deletedInstance;

  @Before
  public void setUp() throws IOException {
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    deletedInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)))
      .setDeleted(true)
      .setDiscoverySuppress(true)
      .setStaffSuppress(true);

    instanceUpdateDelegate = Mockito.spy(new InstanceUpdateDelegate(storage));
    precedingSucceedingTitlesHelper = Mockito.spy(new PrecedingSucceedingTitlesHelper(ctxt -> okapiHttpClient));
    updateInstanceEventHandler =
      new UpdateInstanceQuickMarcEventHandler(instanceUpdateDelegate, context, precedingSucceedingTitlesHelper);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(), any());

    doAnswer(invocationOnMock -> {
      Instance instanceRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instanceRecord));
      return null;
    }).when(instanceRecordCollection).update(any(), any(), any());

    when(okapiHttpClient.put(anyString(), any(JsonObject.class)))
      .thenReturn(CompletableFuture.completedFuture(new Response(204, null, null, null)));

    when(context.getTenantId()).thenReturn("dummy");
    when(context.getToken()).thenReturn("token");
    when(context.getOkapiLocation()).thenReturn("http://localhost");

    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    record = new JsonObject(TestUtil.readFileFromPath(RECORD_PATH));
    deletedRecord = new JsonObject(TestUtil.readFileFromPath(DELETED_RECORD_PATH));
  }

  @Test
  public void shouldProcessEvent() {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_BIB");
    eventPayload.put("MARC_BIB", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());
    eventPayload.put("RELATED_RECORD_VERSION", INSTANCE_VERSION);

    Future<Instance> future = updateInstanceEventHandler.handle(eventPayload);
    Instance updatedInstance = future.result();

    Assert.assertNotNull(updatedInstance);
    Assert.assertEquals(INSTANCE_ID, updatedInstance.getId());
    Assert.assertEquals(INSTANCE_VERSION, updatedInstance.getVersion());
    Assert.assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    Assert.assertNotNull(
      updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value)).findFirst().orElse(null));
    Assert.assertNotNull(
      updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst()
        .orElse(null));
    Assert.assertNotNull(updatedInstance.getSubjects());
    Assert.assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().get(0).getValue(), Matchers.containsString("additional subfield"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);
    Assert.assertEquals(false, updatedInstance.getDiscoverySuppress());
    Assert.assertEquals(false, updatedInstance.getStaffSuppress());

    ArgumentCaptor<Context> argument = ArgumentCaptor.forClass(Context.class);
    verify(instanceUpdateDelegate).handle(any(), any(), argument.capture());
    Assert.assertEquals("token", argument.getValue().getToken());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("http://localhost", argument.getValue().getOkapiLocation());
  }

  @Test
  public void shouldCompleteExceptionallyOnOLNumberExceeded() {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_BIB");
    eventPayload.put("MARC_BIB", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());
    eventPayload.put("RELATED_RECORD_VERSION", INSTANCE_VERSION);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", 409));
      return null;
    }).when(instanceRecordCollection).update(any(), any(), any());

    Future<Instance> future = updateInstanceEventHandler.handle(eventPayload);
    verify(instanceRecordCollection, times(1)).update(any(), any(), any());
    Assert.assertTrue(future.failed());
    Assert.assertTrue(future.cause() instanceof OptimisticLockingException);
  }

  @Test
  public void shouldAddPrecedingAndSucceedingTitlesFromIncomingRecord() throws IOException {
    Record record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.getParsedRecord().withContent(PARSED_CONTENT_WITH_PRECEDING_SUCCEEDING_TITLES);

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_BIB");
    eventPayload.put("MARC_BIB", Json.encode(record));
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());
    eventPayload.put("RELATED_RECORD_VERSION", INSTANCE_VERSION);

    Future<Instance> future = updateInstanceEventHandler.handle(eventPayload);
    Instance updatedInstance = future.result();

    Assert.assertNotNull(updatedInstance);
    Assert.assertEquals(INSTANCE_ID, updatedInstance.getId());
    Assert.assertEquals(INSTANCE_VERSION, updatedInstance.getVersion());
    Assert.assertTrue(existingInstance.getPrecedingTitles().isEmpty());
    Assert.assertTrue(existingInstance.getSucceedingTitles().isEmpty());
    Assert.assertEquals(1, updatedInstance.getPrecedingTitles().size());
    Assert.assertEquals("Houston oil directory",
      updatedInstance.getPrecedingTitles().get(0).toPrecedingTitleJson().getString(TITLE_KEY));
    Assert.assertEquals(1, updatedInstance.getSucceedingTitles().size());
    Assert.assertEquals("SAIS review of international affairs",
      updatedInstance.getSucceedingTitles().get(0).toSucceedingTitleJson().getString(TITLE_KEY));
    verify(precedingSucceedingTitlesHelper).updatePrecedingSucceedingTitles(any(Instance.class), any(Context.class));
  }

  @Test
  public void shouldKeepInstanceSuppressedIfRecordLeaderUpdatedToNotDeleted() {
    HashMap<String, String> eventPayload = new HashMap<>();
    JsonObject staffSuppressedRecord = JsonObject.mapFrom(record.mapTo(Record.class)
      .withAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(true))
      .withDeleted(false));

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(deletedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(), any());

    eventPayload.put("RECORD_TYPE", "MARC_BIB");
    eventPayload.put("MARC_BIB", staffSuppressedRecord.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());
    eventPayload.put("RELATED_RECORD_VERSION", INSTANCE_VERSION);

    Future<Instance> future = updateInstanceEventHandler.handle(eventPayload);
    Instance updatedInstance = future.result();

    Assert.assertNotNull(updatedInstance);
    Assert.assertEquals(INSTANCE_ID, updatedInstance.getId());
    Assert.assertEquals(INSTANCE_VERSION, updatedInstance.getVersion());
    Assert.assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    Assert.assertNotNull(
      updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value)).findFirst().orElse(null));
    Assert.assertNotNull(
      updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst()
        .orElse(null));
    Assert.assertNotNull(updatedInstance.getSubjects());
    Assert.assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().get(0).getValue(), Matchers.containsString("additional subfield"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);
    Assert.assertEquals(true, updatedInstance.getDiscoverySuppress());
    Assert.assertEquals(true, updatedInstance.getStaffSuppress());

    ArgumentCaptor<Context> argument = ArgumentCaptor.forClass(Context.class);
    verify(instanceUpdateDelegate).handle(any(), any(), argument.capture());
    Assert.assertEquals("token", argument.getValue().getToken());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("http://localhost", argument.getValue().getOkapiLocation());
  }

  @Test
  public void shouldSetInstanceSuppressedIfRecordLeaderUpdatedToDeleted() {
    HashMap<String, String> eventPayload = new HashMap<>();

    eventPayload.put("RECORD_TYPE", "MARC_BIB");
    eventPayload.put("MARC_BIB", deletedRecord.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());
    eventPayload.put("RELATED_RECORD_VERSION", INSTANCE_VERSION);

    Future<Instance> future = updateInstanceEventHandler.handle(eventPayload);
    Instance updatedInstance = future.result();

    Assert.assertNotNull(updatedInstance);
    Assert.assertEquals(INSTANCE_ID, updatedInstance.getId());
    Assert.assertEquals(INSTANCE_VERSION, updatedInstance.getVersion());
    Assert.assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    Assert.assertNotNull(
      updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value)).findFirst().orElse(null));
    Assert.assertNotNull(
      updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst()
        .orElse(null));
    Assert.assertNotNull(updatedInstance.getSubjects());
    Assert.assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().get(0).getValue(), Matchers.containsString("additional subfield"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);
    Assert.assertEquals(true, updatedInstance.getDiscoverySuppress());
    Assert.assertEquals(true, updatedInstance.getStaffSuppress());

    ArgumentCaptor<Context> argument = ArgumentCaptor.forClass(Context.class);
    verify(instanceUpdateDelegate).handle(any(), any(), argument.capture());
    Assert.assertEquals("token", argument.getValue().getToken());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("http://localhost", argument.getValue().getOkapiLocation());
  }

  @Test
  public void shouldKeepSuppressedFlagsIfRecordWithDeletedLeaderUpdated() {
    HashMap<String, String> eventPayload = new HashMap<>();

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(deletedInstance.copyInstance()
        .setStaffSuppress(false)
        .setDiscoverySuppress(true)));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(), any());

    eventPayload.put("RECORD_TYPE", "MARC_BIB");
    eventPayload.put("MARC_BIB", deletedRecord.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());
    eventPayload.put("RELATED_RECORD_VERSION", INSTANCE_VERSION);

    Future<Instance> future = updateInstanceEventHandler.handle(eventPayload);
    Instance updatedInstance = future.result();

    Assert.assertNotNull(updatedInstance);
    Assert.assertEquals(INSTANCE_ID, updatedInstance.getId());
    Assert.assertEquals(INSTANCE_VERSION, updatedInstance.getVersion());
    Assert.assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    Assert.assertNotNull(
      updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value)).findFirst().orElse(null));
    Assert.assertNotNull(
      updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst()
        .orElse(null));
    Assert.assertNotNull(updatedInstance.getSubjects());
    Assert.assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().get(0).getValue(), Matchers.containsString("additional subfield"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);
    Assert.assertEquals(true, updatedInstance.getDiscoverySuppress());
    Assert.assertEquals(false, updatedInstance.getStaffSuppress());

    ArgumentCaptor<Context> argument = ArgumentCaptor.forClass(Context.class);
    verify(instanceUpdateDelegate).handle(any(), any(), argument.capture());
    Assert.assertEquals("token", argument.getValue().getToken());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("http://localhost", argument.getValue().getOkapiLocation());
  }

  @Test
  public void shouldCompleteExceptionally_whenRecordIsEmpty() {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_BIB");
    eventPayload.put("MARC_BIB", "");
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<Instance> future =
      updateInstanceEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
  }

  @Test
  public void shouldSendError() {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(instanceRecordCollection).update(any(), any(), any());

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_BIB");
    eventPayload.put("MARC_BIB", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<Instance> future =
      updateInstanceEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
  }

}
