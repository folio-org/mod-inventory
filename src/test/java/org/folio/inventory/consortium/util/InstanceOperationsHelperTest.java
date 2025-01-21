package org.folio.inventory.consortium.util;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.handlers.TenantProvider;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.function.Consumer;

import static org.folio.inventory.dataimport.util.DataImportConstants.ALREADY_EXISTS_ERROR_MSG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class InstanceOperationsHelperTest {

  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private InstanceOperationsHelper instanceOperationsHelper;
  @Mock
  private TenantProvider tenantProvider;
  @Mock
  private InstanceCollection instanceCollection;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    when(tenantProvider.getInstanceCollection()).thenReturn(instanceCollection);
    instanceOperationsHelper = new InstanceOperationsHelper();
  }

  @Test
  public void addInstanceSuccessTest() throws IOException {
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
  public void addInstanceFailureTest() throws Exception {
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    Instance existingInstance = Instance.fromJson(jsonInstance);

    doAnswer(invocation -> {
      Consumer<Failure> failureHandler = invocation.getArgument(2);
      failureHandler.accept(new Failure(String.format(ALREADY_EXISTS_ERROR_MSG, existingInstance.getId()), 400));
      return null;
    }).when(instanceCollection).add(eq(existingInstance), any(), any());

    instanceOperationsHelper.addInstance(existingInstance, tenantProvider)
      .onComplete(result -> {
        assertTrue(result.failed());
        assertEquals("Duplicated event by InstanceId=" + existingInstance.getId(), result.cause().getMessage());
      });
  }
}
