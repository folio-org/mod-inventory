package org.folio.inventory.eventhandlers.preloaders;

import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.inventory.dataimport.handlers.matching.preloaders.InstancePreloader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.OrdersPreloaderHelper;
import org.folio.inventory.dataimport.handlers.matching.preloaders.PreloadingFields;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.value.ListValue;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

@RunWith(MockitoJUnitRunner.class)
public class InstancePreloaderTest {

    private static final List<String> POLS = List.of("10001-1", "10001-2");

    @Mock
    private MarcValueReaderImpl marcValueReader;
    @Mock
    private OrdersPreloaderHelper ordersPreloaderHelper;
    @InjectMocks
    private final InstancePreloader preloader = new InstancePreloader(ordersPreloaderHelper);

    @Before
    @SneakyThrows
    public void setUp() {
        MatchValueReaderFactory.clearReaderFactory();
        when(marcValueReader.isEligibleForEntityType(MARC_BIBLIOGRAPHIC)).thenReturn(true);
        when(marcValueReader.read(any(DataImportEventPayload.class), any(MatchDetail.class)))
                .thenReturn(ListValue.of(POLS));
        MatchValueReaderFactory.register(marcValueReader);
    }

    @Test
    @SneakyThrows
    public void shouldPreloadByPOL() {
        MatchExpression incomingMatchExpression = new MatchExpression()
                .withDataValueType(VALUE_FROM_RECORD)
                .withFields(List.of(
                        new Field().withLabel("field").withValue("935"),
                        new Field().withLabel("indicator1").withValue(""),
                        new Field().withLabel("indicator2").withValue(""),
                        new Field().withLabel("recordSubfield").withValue("a")
                ));
        DataImportEventPayload eventPayload = createEventPayload();
        MatchDetail matchDetail =((MatchProfile) eventPayload.getCurrentNode().getContent()).getMatchDetails().get(0);
        matchDetail.setIncomingMatchExpression(incomingMatchExpression);

        List<String> instanceIdsMock = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        when(ordersPreloaderHelper.preload(eq(eventPayload), eq(PreloadingFields.POL), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(instanceIdsMock));

        LoadQuery initialLoadQuery = LoadQueryBuilder.build(ListValue.of(POLS), matchDetail);
        LoadQuery loadQuery = preloader.preload(initialLoadQuery, eventPayload)
                .get(20, TimeUnit.SECONDS);

        Assertions.assertThat(loadQuery.getCql()).isNotEqualTo(initialLoadQuery.getCql());
        Assertions.assertThat(loadQuery.getCql())
                .isEqualTo(String.format("(id == \"%s\" OR id == \"%s\")", instanceIdsMock.get(0), instanceIdsMock.get(1)));
    }

    @Test
    @SneakyThrows
    public void shouldReturnNullForNullLoadQuery() {
        LoadQuery loadQuery = preloader.preload(null, createEventPayload())
                .get(20, TimeUnit.SECONDS);

        Assertions.assertThat(loadQuery).isNull();
    }

    @Test
    @SneakyThrows
    public void shouldReturnInitialQueryIfPreloadingFieldDoesNotExistInMatchExpression() {
        MatchExpression existingMatchExpression = new MatchExpression()
                .withDataValueType(VALUE_FROM_RECORD)
                .withFields(singletonList(
                        new Field().withLabel("field").withValue("instance.id"))
                );
        DataImportEventPayload eventPayload = createEventPayload();
        MatchDetail matchDetail =((MatchProfile) eventPayload.getCurrentNode().getContent()).getMatchDetails().get(0);
        matchDetail.setExistingMatchExpression(existingMatchExpression);

        LoadQuery initialLoadQuery = LoadQueryBuilder.build(ListValue.of(POLS), matchDetail);
        LoadQuery loadQuery = preloader.preload(initialLoadQuery, eventPayload)
                .get(20, TimeUnit.SECONDS);

        Assertions.assertThat(loadQuery.getCql()).isEqualTo(initialLoadQuery.getCql());
    }

    private DataImportEventPayload createEventPayload() {
        return new DataImportEventPayload()
                .withOkapiUrl("http://localhost:9493")
                .withTenant("diku")
                .withToken("token")
                .withCurrentNode(new ProfileSnapshotWrapper()
                        .withId(UUID.randomUUID().toString())
                        .withContentType(MATCH_PROFILE)
                        .withContent(new MatchProfile()
                                .withExistingRecordType(INSTANCE)
                                .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
                                .withMatchDetails(singletonList(new MatchDetail()
                                        .withMatchCriterion(EXACTLY_MATCHES)
                                        .withExistingMatchExpression(new MatchExpression()
                                                .withDataValueType(VALUE_FROM_RECORD)
                                                .withFields(singletonList(
                                                        new Field().withLabel("field").withValue("instance.purchaseOrderLineNumber"))
                                                ))))));
    }
}
