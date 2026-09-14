package org.folio.inventory.consortium.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.exceptions.StorageOperationException;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.domain.instances.Instance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
class FolioInstanceSharingHandlerImplTest {

  private static final String INSTANCE_ID = "eb89b292-d2b7-4c36-9bfc-f816d6f96418";
  private static final String TARGET_INSTANCE_HRID = "consin0000000000101";
  private static final String CONSORTIUM_TENANT = "consortium";
  private static final String MEMBER_TENANT = "diku";

  @Mock
  private InstanceOperationsHelper instanceOperationsHelper;
  @Mock
  private SourceTenantProvider sourceTenantProvider;
  @Mock
  private TargetTenantProvider targetTenantProvider;
  @Mock
  private SharingInstance sharingInstanceMetadata;

  private FolioInstanceSharingHandlerImpl folioHandler;
  private Instance sourceInstance;
  private Map<String, String> kafkaHeaders;

  @BeforeEach
  void setUp() {
    kafkaHeaders = new HashMap<>();
    sourceInstance = new Instance(INSTANCE_ID, 1, "in001", "FOLIO", "testTitle", UUID.randomUUID().toString());

    when(sourceTenantProvider.tenantId()).thenReturn(MEMBER_TENANT);
    when(targetTenantProvider.tenantId()).thenReturn(CONSORTIUM_TENANT);
    when(sharingInstanceMetadata.getInstanceIdentifier()).thenReturn(UUID.fromString(INSTANCE_ID));
    when(sharingInstanceMetadata.getSourceTenantId()).thenReturn(MEMBER_TENANT);
    when(sharingInstanceMetadata.getTargetTenantId()).thenReturn(CONSORTIUM_TENANT);

    folioHandler = new FolioInstanceSharingHandlerImpl(instanceOperationsHelper);
  }

  @Test
  void shouldNotRollbackWhenSharingSucceeds(VertxTestContext testContext) {
    //given
    var targetInstance =
      new Instance(INSTANCE_ID, 1, TARGET_INSTANCE_HRID, "FOLIO", "testTitle", UUID.randomUUID().toString());
    when(instanceOperationsHelper.addInstance(any(), any())).thenReturn(Future.succeededFuture(targetInstance));
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture(INSTANCE_ID));

    // when
    var future = folioHandler.publishInstance(sourceInstance, sharingInstanceMetadata, sourceTenantProvider,
      targetTenantProvider, kafkaHeaders);

    //then
    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(INSTANCE_ID, result);
      verify(instanceOperationsHelper, never()).deleteInstance(any(), any());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldRollbackSharedInstanceWhenSourceInstanceUpdateFails(VertxTestContext testContext) {
    //given
    var targetInstance =
      new Instance(INSTANCE_ID, 1, TARGET_INSTANCE_HRID, "FOLIO", "testTitle", UUID.randomUUID().toString());
    var updateFailure = new StorageOperationException("cannot update source instance", 500);

    when(instanceOperationsHelper.addInstance(any(), any())).thenReturn(Future.succeededFuture(targetInstance));
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.failedFuture(updateFailure));
    when(instanceOperationsHelper.deleteInstance(any(), any())).thenReturn(Future.succeededFuture());

    // when
    var future = folioHandler.publishInstance(sourceInstance, sharingInstanceMetadata, sourceTenantProvider,
      targetTenantProvider, kafkaHeaders);

    //then
    future.onComplete(testContext.failing(cause -> testContext.verify(() -> {
      assertSame(updateFailure, cause);
      verify(instanceOperationsHelper)
        .deleteInstance(eq(INSTANCE_ID), argThat(p -> CONSORTIUM_TENANT.equals(p.tenantId())));
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReportOriginalCauseWhenRollbackItselfFails(VertxTestContext testContext) {
    //given
    var targetInstance =
      new Instance(INSTANCE_ID, 1, TARGET_INSTANCE_HRID, "FOLIO", "testTitle", UUID.randomUUID().toString());
    var updateFailure = new StorageOperationException("cannot update source instance", 500);

    when(instanceOperationsHelper.addInstance(any(), any())).thenReturn(Future.succeededFuture(targetInstance));
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.failedFuture(updateFailure));
    when(instanceOperationsHelper.deleteInstance(any(), any()))
      .thenReturn(Future.failedFuture(new StorageOperationException("cannot delete instance", 500)));

    // when
    var future = folioHandler.publishInstance(sourceInstance, sharingInstanceMetadata, sourceTenantProvider,
      targetTenantProvider, kafkaHeaders);

    //then: the rollback failure must not mask why sharing failed
    future.onComplete(testContext.failing(cause -> testContext.verify(() -> {
      assertSame(updateFailure, cause);
      testContext.completeNow();
    })));
  }
}
