package org.folio.inventory.dataimport.handlers.actions;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.google.common.collect.Lists;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.MappingMetadataDto;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.services.OrderHelperServiceImpl;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.InstanceIdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.util.concurrent.CompletableFuture.completedStage;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_005;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.dateTime005Formatter;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateInstanceEventHandlerTest {

  private static final String PARSED_CONTENT = """
    {
      "id": "c56b70ce-4ef6-47ef-8bc3-c470bafa0b8c",
      "externalIdsHolder": {
        "instanceId": "b9a5f035-de63-4e2c-92c2-07240c89b817"
      },
      "recordType": "MARC_BIB",
      "parsedRecord": {
        "id": "c9db5d7a-e1d4-11e8-9f32-f2801f1b9fd1",
        "content": {
          "fields": [
            {
              "066": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "c": "$1"
                  },
                  {
                    "c": "(3"
                  }
                ]
              }
            },
            {
              "001": "393893"
            },
            {
              "005": "20141107001016.0"
            },
            {
              "006": "c bcdefghijklmn o "
            },
            {
              "007": "sa bcdefghijkl"
            },
            {
              "008": "abcdefghijklmnopqr bcdefghijklmn o stuvw"
            },
            {
              "010": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "  2001000234"
                  }
                ]
              }
            },
            {
              "035": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "(OCoLC)63611770"
                  }
                ]
              }
            },
            {
              "035": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "393893"
                  }
                ]
              }
            },
            {
              "040": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "c": "UPB"
                  },
                  {
                    "d": "UPB"
                  },
                  {
                    "d": "NIC"
                  },
                  {
                    "d": "NIC"
                  }
                ]
              }
            },
            {
              "041": {
                "ind1": "0",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "latitager"
                  },
                  {
                    "g": "ger"
                  }
                ]
              }
            },
            {
              "045": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "v6v9"
                  }
                ]
              }
            },
            {
              "047": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "cn"
                  },
                  {
                    "a": "ct"
                  },
                  {
                    "a": "co"
                  },
                  {
                    "a": "df"
                  },
                  {
                    "a": "dv"
                  },
                  {
                    "a": "ft"
                  },
                  {
                    "a": "fg"
                  },
                  {
                    "a": "ms"
                  },
                  {
                    "a": "mi"
                  },
                  {
                    "a": "nc"
                  },
                  {
                    "a": "op"
                  },
                  {
                    "a": "ov"
                  },
                  {
                    "a": "rq"
                  },
                  {
                    "a": "sn"
                  },
                  {
                    "a": "su"
                  },
                  {
                    "a": "sy"
                  },
                  {
                    "a": "vr"
                  },
                  {
                    "a": "zz"
                  }
                ]
              }
            },
            {
              "050": {
                "ind1": "0",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "M3"
                  },
                  {
                    "b": ".M896"
                  }
                ]
              }
            },
            {
              "100": {
                "ind1": "/",
                "ind2": "/",
                "subfields": [
                  {
                    "a": "Mozart, Wolfgang Amadeus,"
                  },
                  {
                    "d": "1756-1791."
                  },
                  {
                    "0": "12345"
                  },
                  {
                    "9": "b9a5f035-de63-4e2c-92c2-07240c88b817"
                  }
                ]
              }
            },
            {
              "240": {
                "ind1": "1",
                "ind2": "0",
                "subfields": [
                  {
                    "a": "Works"
                  }
                ]
              }
            },
            {
              "245": {
                "ind1": "1",
                "ind2": "0",
                "subfields": [
                  {
                    "a": "Neue Ausgabe samtlicher Werke,"
                  },
                  {
                    "b": "in Verbindung mit den Mozartstadten, Augsburg, Salzburg und Wien."
                  },
                  {
                    "c": "Hrsg. von der Internationalen Stiftung Mozarteum, Salzburg."
                  }
                ]
              }
            },
            {
              "246": {
                "ind1": "3",
                "ind2": "3",
                "subfields": [
                  {
                    "a": "Neue Mozart-Ausgabe"
                  }
                ]
              }
            },
            {
              "260": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "Kassel,"
                  },
                  {
                    "b": "Barenreiter,"
                  },
                  {
                    "c": "c1955-"
                  }
                ]
              }
            },
            {
              "300": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "v."
                  },
                  {
                    "b": "facsims."
                  },
                  {
                    "c": "33 cm."
                  }
                ]
              }
            },
            {
              "505": {
                "ind1": "0",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "Ser. I. Geistliche Gesangswerke -- Ser. II. Opern -- Ser. III. Lieder, mehrstimmige Gesange, Kanons -- Ser. IV. Orchesterwerke -- Ser. V. Konzerte -- Ser. VI. Kirchensonaten -- Ser. VII. Ensemblemusik fur grossere Solobesetzungen -- Ser. VIII. Kammermusik -- Ser. IX. Klaviermusik -- Ser. X. Supplement."
                  }
                ]
              }
            },
            {
              "590": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "Purchase price: $325.00, 1980 August."
                  }
                ]
              }
            },
            {
              "650": {
                "ind1": " ",
                "ind2": "0",
                "subfields": [
                  {
                    "a": "Vocal music"
                  }
                ]
              }
            },
            {
              "650": {
                "ind1": " ",
                "ind2": "0",
                "subfields": [
                  {
                    "a": "Instrumental music"
                  }
                ]
              }
            },
            {
              "650": {
                "ind1": " ",
                "ind2": "7",
                "subfields": [
                  {
                    "a": "Instrumental music"
                  },
                  {
                    "2": "fast"
                  },
                  {
                    "0": "(OCoLC)fst00974414"
                  }
                ]
              }
            },
            {
              "650": {
                "ind1": " ",
                "ind2": "7",
                "subfields": [
                  {
                    "a": "Vocal music"
                  },
                  {
                    "2": "fast"
                  },
                  {
                    "0": "(OCoLC)fst01168379"
                  }
                ]
              }
            },
            {
              "880": {
                "ind1": "0",
                "ind2": "0",
                "subfields": [
                  {
                    "6": "245-01/$1"
                  },
                  {
                    "a": "abcde /"
                  },
                  {
                    "c": "fghij"
                  }
                ]
              }
            },
            {
              "902": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "pfnd"
                  },
                  {
                    "b": "Austin Music"
                  }
                ]
              }
            },
            {
              "905": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "19980728120000.0"
                  }
                ]
              }
            },
            {
              "948": {
                "ind1": "1",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "20100622"
                  },
                  {
                    "b": "s"
                  },
                  {
                    "d": "lap11"
                  },
                  {
                    "e": "lts"
                  },
                  {
                    "x": "ToAddCatStat"
                  }
                ]
              }
            },
            {
              "948": {
                "ind1": "0",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "20110818"
                  },
                  {
                    "b": "r"
                  },
                  {
                    "d": "np55"
                  },
                  {
                    "e": "lts"
                  }
                ]
              }
            },
            {
              "948": {
                "ind1": "2",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "20130128"
                  },
                  {
                    "b": "m"
                  },
                  {
                    "d": "bmt1"
                  },
                  {
                    "e": "lts"
                  }
                ]
              }
            },
            {
              "948": {
                "ind1": "2",
                "ind2": " ",
                "subfields": [
                  {
                    "a": "20141106"
                  },
                  {
                    "b": "m"
                  },
                  {
                    "d": "batch"
                  },
                  {
                    "e": "lts"
                  },
                  {
                    "x": "addfast"
                  }
                ]
              }
            }
          ],
          "leader": "01750ccm a2200421   4500"
        }
      },
      "additionalInfo": {
        "suppressDiscovery": false
      },
      "recordState": "ACTUAL",
      "metadata": {
        "updatedDate": "2020-07-16T15:13:36.879+03:00",
        "updatedByUserId": "38d3a441-c100-5e8d-bd12-71bde492b723"
      }
    }
    """;
  private static final String PARSED_CONTENT_999ffi = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"titleValue\"}]}},{\"336\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"b\":\"b6698d38-149f-11ec-82a8-0242ac130003\"}]}},{\"780\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"Houston oil directory\"}]}},{\"785\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"SAIS review of international affairs\"},{\"x\":\"1945-4724\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Adaptation of Xi xiang ji by Wang Shifu.\"}]}},{\"520\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\"}]}}]}";
  private static final String PARSED_CONTENT_WITH_005 = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"005\":\"20141107001016.0\"},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"titleValue\"}]}},{\"336\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"b\":\"b6698d38-149f-11ec-82a8-0242ac130003\"}]}},{\"780\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"Houston oil directory\"}]}},{\"785\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"SAIS review of international affairs\"},{\"x\":\"1945-4724\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Adaptation of Xi xiang ji by Wang Shifu.\"}]}},{\"520\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\"}]}}]}";
  private static final String PARSED_CONTENT_WITH_999fi = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"003\":\"in001\"},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"titleValue\"}]}},{\"336\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"b\":\"b6698d38-149f-11ec-82a8-0242ac130003\"}]}},{\"780\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"Houston oil directory\"}]}},{\"785\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"SAIS review of international affairs\"},{\"x\":\"1945-4724\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Adaptation of Xi xiang ji by Wang Shifu.\"}]}},{\"520\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\"}]}} , {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"i\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } }]}";
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "dummy";

  @Mock
  private Storage storage;
  @Mock
  InstanceCollection instanceRecordCollection;
  @Mock
  OkapiHttpClient mockedClient;
  @Mock
  private InstanceIdStorageService instanceIdStorageService;
  @Mock
  private OrderHelperServiceImpl orderHelperService;
  @Mock
  private SourceStorageRecordsClient sourceStorageClient;
  @Spy
  private MarcBibReaderFactory fakeReaderFactory = new MarcBibReaderFactory();

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create preliminary Item")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(INSTANCE);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"))));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile)
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  private CreateInstanceEventHandler createInstanceEventHandler;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();

    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.toString())))));

    Vertx vertx = Vertx.vertx();
    HttpClient httpClient = vertx.createHttpClient();
    createInstanceEventHandler = spy(new CreateInstanceEventHandler(storage,
      new PrecedingSucceedingTitlesHelper(context -> mockedClient), new MappingMetadataCache(vertx,
      httpClient, 3600), instanceIdStorageService, orderHelperService, httpClient));

    doReturn(sourceStorageClient).when(createInstanceEventHandler).getSourceStorageRecordsClient(any(), any());
    doAnswer(invocationOnMock -> {
      Instance instanceRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instanceRecord));
      return null;
    }).when(instanceRecordCollection).add(any(), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> completedStage(createdResponse()))
      .when(mockedClient).post(any(URL.class), any(JsonObject.class));

    when(orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(any(), any(), any())).thenReturn(Future.succeededFuture());
  }

  @Test
  public void shouldProcessEventWithout999() throws InterruptedException, ExecutionException, TimeoutException {
    shouldProcessEvent(PARSED_CONTENT, Boolean.FALSE.toString());
  }

  @Test
  public void shouldProcessEventWith999AndAcceptedInstanceId() throws InterruptedException, ExecutionException, TimeoutException {
    shouldProcessEvent(PARSED_CONTENT_WITH_999fi, Boolean.TRUE.toString());
  }

  public void shouldProcessEvent(String content, String acceptInstanceId) throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = "fe19bae4-da28-472b-be90-d442e2428ead";
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    String title = "titleValue";
    RecordToEntity recordToInstance = RecordToEntity.builder().recordId(recordId).entityId(instanceId).build();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    when(instanceIdStorageService.store(any(), any(), any())).thenReturn(Future.succeededFuture(recordToInstance));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(content));
    record.setId(recordId);

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put("acceptInstanceId", acceptInstanceId);

    Buffer buffer = BufferImpl.buffer("{\"parsedRecord\":{" +
      "\"id\":\"990fad8b-64ec-4de4-978c-9f8bbed4c6d3\"," +
      "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" +
      "}}");
    HttpResponse<Buffer> resp = buildHttpResponseWithBuffer(buffer);
    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(resp));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_CREATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    String actualInstanceId = createdInstance.getString("id");
    assertNotNull(actualInstanceId);
    assertEquals(instanceId, actualInstanceId);
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals("MARC", createdInstance.getString("source"));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(createInstanceEventHandler).getSourceStorageRecordsClient(any(), argThat(tenantId -> tenantId.equals(TENANT_ID)));
  }

  @Test
  public void shouldProcessEventAndUpdate005Field() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = "fe19bae4-da28-472b-be90-d442e2428ead";
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    String title = "titleValue";
    RecordToEntity recordToInstance = RecordToEntity.builder().recordId(recordId).entityId(instanceId).build();

    String expectedDate = dateTime005Formatter.format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(instanceIdStorageService.store(any(), any(), any())).thenReturn(Future.succeededFuture(recordToInstance));
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HttpResponse<Buffer> resp = buildHttpResponseWithBuffer(BufferImpl.buffer("{}"));
    ArgumentCaptor<Record> recordCaptor = ArgumentCaptor.forClass(Record.class);
    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(resp));

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_005));
    record.setId(recordId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    verify(sourceStorageClient).postSourceStorageRecords(recordCaptor.capture());
    assertEquals(DI_INVENTORY_INSTANCE_CREATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    String actualInstanceId = createdInstance.getString("id");
    assertNotNull(actualInstanceId);
    assertEquals(instanceId, actualInstanceId);
    String actualDate = AdditionalFieldsUtil.getValueFromControlledField(recordCaptor.getValue(), TAG_005);
    assertNotNull(actualDate);
    assertEquals(expectedDate.substring(0, 10), actualDate.substring(0, 10));
    assertEquals(recordId, recordCaptor.getValue().getMatchedId());
    assertEquals(instanceId, recordCaptor.getValue().getExternalIdsHolder().getInstanceId());
  }

  @Test(expected = ExecutionException.class)
  public void shouldProcessEventAndDeleteInstanceIfFailedCreateRecord() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = "fe19bae4-da28-472b-be90-d442e2428ead";
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    String title = "titleValue";
    RecordToEntity recordToInstance = RecordToEntity.builder().recordId(recordId).entityId(instanceId).build();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(instanceIdStorageService.store(any(), any(), any())).thenReturn(Future.succeededFuture(recordToInstance));
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HttpResponseImpl<Buffer> response = new HttpResponseImpl<>(null, HttpStatus.SC_BAD_REQUEST, "",
      null, null, null, BufferImpl.buffer("{}"), null);
    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(response));

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_005));
    record.setId(recordId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldProcessConsortiumEvent() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = "fe19bae4-da28-472b-be90-d442e2428ead";
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "957985c6-97e3-4038-b0e7-343ecd0b8120";
    String title = "titleValue";
    RecordToEntity recordToInstance = RecordToEntity.builder().recordId(recordId).entityId(instanceId).build();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    when(instanceIdStorageService.store(any(), any(), any())).thenReturn(Future.succeededFuture(recordToInstance));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_999ffi));
    record.setId(recordId);

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    Buffer buffer = BufferImpl.buffer("{\"id\":\"567859ad-505a-400d-a699-0028a1fdbf84\",\"parsedRecord\":{\"content\":\"{\\\"leader\\\":\\\"00567nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"957985c6-97e3-4038-b0e7-343ecd0b8120\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"},\"deleted\":false,\"state\":\"ACTUAL\"}");
    HttpResponse<Buffer> resp = buildHttpResponseWithBuffer(buffer);
    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(resp));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_CREATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    String actualInstanceId = createdInstance.getString("id");
    assertNotNull(actualInstanceId);
    assertEquals(instanceId, actualInstanceId);
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals("MARC", createdInstance.getString("source"));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsNull() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(null)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfMArcBibliographicIsNotExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put("InvalidField", Json.encode(new Record()));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfMarcBibliographicIsEmptyInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = Exception.class)
  public void shouldNotProcessEventIfRequiredFieldIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), MissingValue.getInstance());

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(new JsonObject()))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = Exception.class)
  public void shouldNotProcessEventIfRecordContains999field() throws InterruptedException, ExecutionException, TimeoutException {
    var recordId = UUID.randomUUID().toString();

    HttpResponse<Buffer> resp = buildHttpResponseWithBuffer(BufferImpl.buffer("{}"));
    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(resp));

    var context = new HashMap<String, String>();
    var record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_999fi));
    record.setId(recordId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    var dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl());

    var future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(20, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() {
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile));

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals("Action profile to create an Instance requires a mapping profile by jobExecutionId: 'null' and recordId: 'null'", exception.getCause().getMessage());
  }

  @Test
  public void isEligibleShouldReturnTrue() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotInstance() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isPostProcessingNeededShouldReturnTrue() {
    assertFalse(createInstanceEventHandler.isPostProcessingNeeded());
  }

  @Test
  public void shouldReturnPostProcessingInitializationEventType() {
    assertEquals(DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value(), createInstanceEventHandler.getPostProcessingInitializationEventType());
  }

  @Test(expected = Exception.class)
  public void shouldNotProcessEventWhenRecordToInstanceFutureFails() throws ExecutionException, InterruptedException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = "fe19bae4-da28-472b-be90-d442e2428ead";
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    when(instanceIdStorageService.store(any(), any(), any())).thenReturn(Future.failedFuture(new Exception()));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    record.setId(recordId);

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }


  @Test(expected = Exception.class)
  public void shouldNotProcessEventEvenIfDuplicatedInventoryStorageErrorExists() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = "fe19bae4-da28-472b-be90-d442e2428ead";
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    String title = "titleValue";
    RecordToEntity recordToInstance = RecordToEntity.builder().recordId(recordId).entityId(instanceId).build();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    when(instanceIdStorageService.store(any(), any(), any())).thenReturn(Future.succeededFuture(recordToInstance));
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(UNIQUE_ID_ERROR_MESSAGE, 400));
      return null;
    }).when(instanceRecordCollection).add(any(), any(), any());

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    record.setId(recordId);

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);

    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = Exception.class)
  public void shouldNotProcessEventEvenIfInventoryStorageErrorExists() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = "fe19bae4-da28-472b-be90-d442e2428ead";
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    String title = "titleValue";
    RecordToEntity recordToInstance = RecordToEntity.builder().recordId(recordId).entityId(instanceId).build();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    when(instanceIdStorageService.store(any(), any(), any())).thenReturn(Future.succeededFuture(recordToInstance));
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Smth error", 400));
      return null;
    }).when(instanceRecordCollection).add(any(), any(), any());

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    record.setId(recordId);

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);

    future.get(5, TimeUnit.SECONDS);
  }

  private static HttpResponseImpl<Buffer> buildHttpResponseWithBuffer(Buffer buffer) {
    return new HttpResponseImpl<>(null, HttpStatus.SC_CREATED, "",
      null, null, null, buffer, null);
  }

  private Response createdResponse() {
    return new Response(HttpStatus.SC_CREATED, null, null, null);
  }
}
