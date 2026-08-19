package org.folio.inventory.instanceingress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.jaxrs.model.InstanceIngressEvent.EventType.UPDATE_INSTANCE;
import static org.folio.rest.jaxrs.model.InstanceIngressPayload.SourceType.LINKED_DATA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(VertxUnitRunner.class)
public class InstanceIngressEventConsumerTest {

  private static final String OKAPI_URL = "http://okapi:9130";
  private static final String TOKEN = "test-token";
  private static final String TENANT_ID = "diku";

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private Storage storage;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private InstanceCollection instanceCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;

  @Captor
  private ArgumentCaptor<Context> contextCaptor;

  private Vertx vertx;
  private InstanceIngressEventConsumer consumer;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    when(storage.getInstanceCollection(any())).thenReturn(instanceCollection);
    when(mappingMetadataCache.getByRecordType(anyString(), any(), anyString()))
      .thenReturn(Future.succeededFuture(Optional.empty()));
    consumer = new InstanceIngressEventConsumer(vertx, storage, vertx.createHttpClient(), mappingMetadataCache);
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  @Test
  public void shouldResolveOkapiUrl_whenHeadersUseStandardCase() {
    // arrange
    when(kafkaRecord.value()).thenReturn(Json.encode(buildUpdateEvent()));
    when(kafkaRecord.headers()).thenReturn(List.of(
      KafkaHeader.header(XOkapiHeaders.URL, OKAPI_URL),
      KafkaHeader.header(XOkapiHeaders.TOKEN, TOKEN),
      KafkaHeader.header(XOkapiHeaders.TENANT, TENANT_ID)
    ));

    // act
    consumer.handle(kafkaRecord);

    // assert
    verify(mappingMetadataCache).getByRecordType(anyString(), contextCaptor.capture(), anyString());
    assertThat(contextCaptor.getValue().getOkapiLocation()).isEqualTo(OKAPI_URL);
  }

  @Test
  public void shouldResolveOkapiUrl_whenHeadersUseLowercaseNames() {
    // arrange
    when(kafkaRecord.value()).thenReturn(Json.encode(buildUpdateEvent()));
    when(kafkaRecord.headers()).thenReturn(List.of(
      KafkaHeader.header(XOkapiHeaders.URL.toLowerCase(), OKAPI_URL),
      KafkaHeader.header(XOkapiHeaders.TOKEN.toLowerCase(), TOKEN),
      KafkaHeader.header(XOkapiHeaders.TENANT.toLowerCase(), TENANT_ID)
    ));

    // act
    consumer.handle(kafkaRecord);

    // assert
    verify(mappingMetadataCache).getByRecordType(anyString(), contextCaptor.capture(), anyString());
    assertThat(contextCaptor.getValue().getOkapiLocation()).isEqualTo(OKAPI_URL);
  }

  private InstanceIngressEvent buildUpdateEvent() {
    return new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventType(UPDATE_INSTANCE)
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject("{}")
        .withSourceType(LINKED_DATA));
  }
}
