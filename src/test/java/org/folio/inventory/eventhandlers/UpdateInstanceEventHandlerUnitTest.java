package org.folio.inventory.eventhandlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.UpdateInstanceEventHandler;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.utils.ZIPArchiver;

public class UpdateInstanceEventHandlerUnitTest {

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
  @Spy
  @InjectMocks
  InstanceUpdateDelegate instanceUpdateDelegate;

  private UpdateInstanceEventHandler updateInstanceEventHandler;
  private JsonObject mappingRules;
  private JsonObject record;
  private Instance existingInstance;
  private Map<String,String> headers = new HashMap<>();

  @Before
  public void setUp() throws IOException {
    headers.put("x-okapi-url", "localhost");
    headers.put("x-okapi-tenant", "dummy");
    headers.put("x-okapi-token", "dummy");
    MockitoAnnotations.initMocks(this);
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    updateInstanceEventHandler = new UpdateInstanceEventHandler(instanceUpdateDelegate, context);
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

    when(context.getTenantId()).thenReturn("dummy");
    when(context.getOkapiLocation()).thenReturn("localhost");

    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    record = new JsonObject(TestUtil.readFileFromPath(RECORD_PATH));
  }

  @Test
  public void shouldProcessEvent() throws InterruptedException, ExecutionException, TimeoutException {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("MARC", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());
    eventPayload.put("USER_CONTEXT", "{\"userId\":\"1\", \"token\":\"token\"}");

    CompletableFuture<Instance> future = updateInstanceEventHandler.handle(eventPayload, headers, Vertx.vertx());
    Instance updatedInstance = future.get(5, TimeUnit.MILLISECONDS);

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
    Assert.assertEquals("1", argument.getValue().getUserId());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("localhost", argument.getValue().getOkapiLocation());
  }

  @Test
  public void shouldCompleteExceptionally() throws IOException {

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("MARC", ZIPArchiver.zip(record.encode()));
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    CompletableFuture<Instance> future = updateInstanceEventHandler.handle(eventPayload, headers, Vertx.vertx());

    Assert.assertTrue(future.isCompletedExceptionally());
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

    CompletableFuture<Instance> future = updateInstanceEventHandler.handle(eventPayload, headers, Vertx.vertx());

    Assert.assertTrue(future.isCompletedExceptionally());
  }

}
