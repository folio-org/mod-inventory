package org.folio.inventory.consortium.util;

import static org.folio.HttpStatus.SC_BAD_REQUEST;
import static org.folio.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.folio.inventory.dataimport.util.DataImportConstants.ALREADY_EXISTS_ERROR_MSG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import java.util.function.Consumer;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.exceptions.StorageOperationException;
import org.folio.inventory.consortium.handlers.TenantProvider;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import support.TestUtil;

@ExtendWith(MockitoExtension.class)
class InstanceOperationsHelperTest {

  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";

  private InstanceOperationsHelper instanceOperationsHelper;

  @Mock
  private TenantProvider tenantProvider;
  @Mock
  private InstanceCollection instanceCollection;

  @BeforeEach
  void setUp() {
    when(tenantProvider.instanceCollection()).thenReturn(instanceCollection);
    instanceOperationsHelper = new InstanceOperationsHelper();
  }

  @Test
  void addInstanceSuccessTest() {
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    Instance existingInstance = Instance.fromJson(jsonInstance);

    doAnswer(invocation -> {
      Consumer<Success<Instance>> successHandler = invocation.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(instanceCollection).add(any(Instance.class), any(), any());

    instanceOperationsHelper.addInstance(existingInstance, tenantProvider)
      .onComplete(result -> {
        Instance addedInstance = result.result();
        assertEquals(existingInstance.getId(), addedInstance.getId());
      });
  }

  @Test
  void deleteInstanceSuccessTest() {
    var instanceId = UUID.randomUUID().toString();

    doAnswer(invocation -> {
      Consumer<Success<Void>> successHandler = invocation.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(instanceCollection).delete(eq(instanceId), any(), any());

    instanceOperationsHelper.deleteInstance(instanceId, tenantProvider)
      .onComplete(result -> assertTrue(result.succeeded()));
  }

  @Test
  void deleteInstanceFailureTest() {
    var instanceId = UUID.randomUUID().toString();

    doAnswer(invocation -> {
      Consumer<Failure> failureHandler = invocation.getArgument(2);
      failureHandler.accept(new Failure("Internal Server Error", SC_INTERNAL_SERVER_ERROR));
      return null;
    }).when(instanceCollection).delete(eq(instanceId), any(), any());

    instanceOperationsHelper.deleteInstance(instanceId, tenantProvider)
      .onComplete(result -> {
        assertTrue(result.failed());
        var cause = assertInstanceOf(StorageOperationException.class, result.cause());
        assertEquals("Internal Server Error", cause.getMessage());
        assertEquals(SC_INTERNAL_SERVER_ERROR, cause.getStatusCode().intValue());
      });
  }

  @Test
  void addInstanceFailureTest() {
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    Instance existingInstance = Instance.fromJson(jsonInstance);

    doAnswer(invocation -> {
      Consumer<Failure> failureHandler = invocation.getArgument(2);
      var failure = new Failure(String.format(ALREADY_EXISTS_ERROR_MSG, existingInstance.getId()), SC_BAD_REQUEST);
      failureHandler.accept(failure);
      return null;
    }).when(instanceCollection).add(eq(existingInstance), any(), any());

    instanceOperationsHelper.addInstance(existingInstance, tenantProvider)
      .onComplete(result -> {
        assertTrue(result.failed());
        assertEquals("Duplicated event by InstanceId=" + existingInstance.getId(), result.cause().getMessage());
      });
  }
}
