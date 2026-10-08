package org.folio.inventory.consortium.handlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.domain.instances.Instance;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class FolioInstanceSharingHandlerImplTest {

  private static final String INSTANCE_ID = "eb89b292-d2b7-4c36-9bfc-f816d6f96418";
  private static final String TARGET_INSTANCE_HRID = "consin0000000000101";
  private static final String CONSORTIUM_TENANT = "consortium";
  private static final String MEMBER_TENANT = "diku";

  @Mock
  private InstanceOperationsHelper instanceOperationsHelper;
  @Mock
  private Source source;
  @Mock
  private Target target;
  @Mock
  private SharingInstance sharingInstanceMetadata;

  private FolioInstanceSharingHandlerImpl folioHandler;
  private Instance sourceInstance;
  private Map<String, String> kafkaHeaders;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    kafkaHeaders = new HashMap<>();
    sourceInstance = new Instance(INSTANCE_ID, 1, "in001", "FOLIO", "testTitle", UUID.randomUUID().toString());

    when(source.getTenantId()).thenReturn(MEMBER_TENANT);
    when(target.getTenantId()).thenReturn(CONSORTIUM_TENANT);
    when(sharingInstanceMetadata.getInstanceIdentifier()).thenReturn(UUID.fromString(INSTANCE_ID));
    when(sharingInstanceMetadata.getSourceTenantId()).thenReturn(MEMBER_TENANT);
    when(sharingInstanceMetadata.getTargetTenantId()).thenReturn(CONSORTIUM_TENANT);
    when(instanceOperationsHelper.republishInstance(any(), any())).thenReturn(Future.succeededFuture());

    folioHandler = new FolioInstanceSharingHandlerImpl(instanceOperationsHelper);
  }

  @Test
  public void shouldNotRollbackWhenSharingSucceeds(TestContext testContext) {
    var async = testContext.async();
    //given
    var targetInstance =
      new Instance(INSTANCE_ID, 1, TARGET_INSTANCE_HRID, "FOLIO", "testTitle", UUID.randomUUID().toString());
    when(instanceOperationsHelper.addInstance(any(), any())).thenReturn(Future.succeededFuture(targetInstance));
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture(INSTANCE_ID));

    // when
    var future = folioHandler.publishInstance(sourceInstance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertEquals(INSTANCE_ID, ar.result());
      verify(instanceOperationsHelper, never()).deleteInstance(any(), any());
      verify(instanceOperationsHelper, never()).republishInstance(any(), any());
      async.complete();
    });
  }

  @Test
  public void shouldRollbackSharedInstanceWhenSourceInstanceUpdateFails(TestContext testContext) {
    var async = testContext.async();
    //given
    var targetInstance =
      new Instance(INSTANCE_ID, 1, TARGET_INSTANCE_HRID, "FOLIO", "testTitle", UUID.randomUUID().toString());
    var updateFailure = new RuntimeException("cannot update source instance");

    when(instanceOperationsHelper.addInstance(any(), any())).thenReturn(Future.succeededFuture(targetInstance));
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.failedFuture(updateFailure));
    when(instanceOperationsHelper.deleteInstance(any(), any())).thenReturn(Future.succeededFuture());

    // when
    var future = folioHandler.publishInstance(sourceInstance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      testContext.assertTrue(updateFailure == ar.cause());
      verify(instanceOperationsHelper)
        .deleteInstance(eq(INSTANCE_ID), argThat(p -> CONSORTIUM_TENANT.equals(p.getTenantId())));
      verify(instanceOperationsHelper).republishInstance(INSTANCE_ID, source);
      async.complete();
    });
  }

  @Test
  public void shouldReportOriginalCauseWhenRollbackItselfFails(TestContext testContext) {
    var async = testContext.async();
    //given
    var targetInstance =
      new Instance(INSTANCE_ID, 1, TARGET_INSTANCE_HRID, "FOLIO", "testTitle", UUID.randomUUID().toString());
    var updateFailure = new RuntimeException("cannot update source instance");

    when(instanceOperationsHelper.addInstance(any(), any())).thenReturn(Future.succeededFuture(targetInstance));
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.failedFuture(updateFailure));
    when(instanceOperationsHelper.deleteInstance(any(), any()))
      .thenReturn(Future.failedFuture(new RuntimeException("cannot delete instance")));

    // when
    var future = folioHandler.publishInstance(sourceInstance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then: the rollback failure must not mask why sharing failed; the target instance is left as is
    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      testContext.assertTrue(updateFailure == ar.cause());
      verify(instanceOperationsHelper, never()).republishInstance(any(), any());
      async.complete();
    });
  }
}
