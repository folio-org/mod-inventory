package org.folio.inventory.service;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.errors.TopicExistsException;
import org.folio.inventory.services.InventoryKafkaTopic;
import org.folio.inventory.services.InventoryKafkaTopicService;
import org.folio.kafka.services.KafkaAdminClientService;
import org.folio.kafka.services.KafkaTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class KafkaAdminClientServiceTest {

  private static final String STUB_TENANT = "foo-tenant";
  private static final Set<String> ALL_EXPECTED_TOPICS = Set.of(
    "folio.Default.foo-tenant.DI_INVENTORY_INSTANCE_CREATED",
    "folio.Default.foo-tenant.DI_INVENTORY_HOLDING_CREATED",
    "folio.Default.foo-tenant.DI_INVENTORY_ITEM_CREATED",
    "folio.Default.foo-tenant.DI_INVENTORY_INSTANCE_MATCHED",
    "folio.Default.foo-tenant.DI_INVENTORY_HOLDING_MATCHED",
    "folio.Default.foo-tenant.DI_INVENTORY_ITEM_MATCHED",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_MATCHED",
    "folio.Default.foo-tenant.DI_INVENTORY_INSTANCE_UPDATED",
    "folio.Default.foo-tenant.DI_INVENTORY_HOLDING_UPDATED",
    "folio.Default.foo-tenant.DI_INVENTORY_ITEM_UPDATED",
    "folio.Default.foo-tenant.DI_INVENTORY_INSTANCE_NOT_MATCHED",
    "folio.Default.foo-tenant.DI_INVENTORY_HOLDING_NOT_MATCHED",
    "folio.Default.foo-tenant.DI_INVENTORY_ITEM_NOT_MATCHED",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED",
    "folio.Default.foo-tenant.DI_INVENTORY_AUTHORITY_UPDATED",
    "folio.Default.foo-tenant.DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING",
    "folio.Default.foo-tenant.DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING",
    "folio.Default.foo-tenant.DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_MODIFIED_PARTITIONS",
    "folio.Default.foo-tenant.inventory.instance_ingress"
  );

  @Mock
  private KafkaAdminClient mockClient;
  @Mock
  private Vertx vertx;
  @Mock
  private InventoryKafkaTopicService inventoryKafkaTopicService;

  @BeforeEach
  void setUp() {
    KafkaTopic[] topicObjects = {
      new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_CREATED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_HOLDING_CREATED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_ITEM_CREATED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_MATCHED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_HOLDING_MATCHED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_ITEM_MATCHED", 1),
      new InventoryKafkaTopic("DI_SRS_MARC_BIB_RECORD_MATCHED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_UPDATED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_HOLDING_UPDATED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_ITEM_UPDATED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_INSTANCE_NOT_MATCHED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_HOLDING_NOT_MATCHED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_ITEM_NOT_MATCHED", 1),
      new InventoryKafkaTopic("DI_SRS_MARC_BIB_RECORD_NOT_MATCHED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_AUTHORITY_UPDATED", 1),
      new InventoryKafkaTopic("DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING", 1),
      new InventoryKafkaTopic("DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING", 1),
      new InventoryKafkaTopic("DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING", 1),
      new InventoryKafkaTopic("DI_SRS_MARC_BIB_RECORD_MODIFIED_PARTITIONS", 1),
      new InventoryKafkaTopic("inventory.instance_ingress", 1)
    };

    when(inventoryKafkaTopicService.createTopicObjects()).thenReturn(topicObjects);
  }

  @Test
  void shouldCreateTopicIfAlreadyExist(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new TopicExistsException("x")))
      .thenReturn(failedFuture(new TopicExistsException("y")))
      .thenReturn(failedFuture(new TopicExistsException("z")))
      .thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.succeeding(notUsed -> testContext.verify(() -> {
        verify(mockClient, times(4)).listTopics();
        verify(mockClient, times(4)).createTopics(anyList());
        verify(mockClient, times(1)).close();
        testContext.completeNow();
      })));
  }

  @Test
  void shouldFailIfExistExceptionIsPermanent(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new TopicExistsException("x")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.failing(e -> testContext.verify(() -> {
        assertThat(e, instanceOf(TopicExistsException.class));
        verify(mockClient, times(1)).close();
        testContext.completeNow();
      })));
  }

  @Test
  void shouldNotCreateTopicOnOther(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new RuntimeException("err msg")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.failing(cause ->
        testContext.verify(() -> {
            assertEquals("err msg", cause.getMessage());
            verify(mockClient, times(1)).close();
            testContext.completeNow();
          }
        )));
  }

  @Test
  void shouldCreateTopicIfNotExist(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.succeeding(notUsed -> testContext.verify(() -> {

        @SuppressWarnings("unchecked") final ArgumentCaptor<List<NewTopic>> createTopicsCaptor = forClass(List.class);

        verify(mockClient, times(1)).createTopics(createTopicsCaptor.capture());
        verify(mockClient, times(1)).close();

        // Only these items are expected, so implicitly checks size of list
        assertThat(getTopicNames(createTopicsCaptor), containsInAnyOrder(ALL_EXPECTED_TOPICS.toArray()));
        testContext.completeNow();
      })));
  }

  private List<String> getTopicNames(ArgumentCaptor<List<NewTopic>> createTopicsCaptor) {
    return createTopicsCaptor.getAllValues().getFirst().stream()
      .map(NewTopic::getName)
      .toList();
  }

  private Future<Void> createKafkaTopicsAsync(KafkaAdminClient client) {
    try (var mocked = mockStatic(KafkaAdminClient.class)) {
      mocked.when(() -> KafkaAdminClient.create(eq(vertx), anyMap())).thenReturn(client);

      return new KafkaAdminClientService(vertx)
        .createKafkaTopics(inventoryKafkaTopicService.createTopicObjects(), STUB_TENANT);
    }
  }
}
