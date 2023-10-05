package org.folio.inventory.consortium.handlers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import org.folio.HttpStatus;
import org.folio.Record;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.consortium.util.RestDataImportHelper;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.folio.inventory.consortium.util.RestDataImportHelperTest.buildHttpResponseWithBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
@RunWith(VertxUnitRunner.class)
public class MarcInstanceSharingHandlerImplTest {

  private MarcInstanceSharingHandlerImpl marcHandler;
  @Mock
  private InstanceOperationsHelper instanceOperationsHelper;
  @Mock
  private SourceStorageRecordsClient sourceStorageClient;
  @Mock
  private RestDataImportHelper restDataImportHelper;

  private Instance instance;
  private SharingInstance sharingInstanceMetadata;
  private Source source;
  private Target target;
  private static Vertx vertx;

  private Map<String, String> kafkaHeaders;

  @BeforeClass
  public static void setUpClass() {
    vertx = Vertx.vertx();
  }

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    instance = mock(Instance.class);

    source = mock(Source.class);
    target = mock(Target.class);

    sharingInstanceMetadata = mock(SharingInstance.class);
    when(sharingInstanceMetadata.getInstanceIdentifier()).thenReturn(UUID.randomUUID());
  }

  private final HttpResponse<Buffer> sourceStorageRecordsResponseBuffer =
    buildHttpResponseWithBuffer(HttpStatus.HTTP_OK, BufferImpl.buffer(recordJson));

  @Test
  public void publishInstanceTest(TestContext context) {
    Async async = context.async();

    String instanceId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String targetInstanceHrid = "consin0000000000101";
    Record record = sourceStorageRecordsResponseBuffer.bodyAsJson(Record.class);

    //given
    kafkaHeaders = new HashMap<>();

    marcHandler = spy(new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, vertx));
    setField(marcHandler, "restDataImportHelper", restDataImportHelper);

    doReturn(sourceStorageClient).when(marcHandler).getSourceStorageRecordsClient(anyString(), eq(kafkaHeaders));
    doReturn(Future.succeededFuture(record)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());

    when(restDataImportHelper.importMarcRecord(any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    doReturn(Future.succeededFuture(instanceId)).when(marcHandler).deleteSourceRecordByInstanceId(any(), any(), any(), any());
    when(instance.getJsonForStorage()).thenReturn((JsonObject) Json.decodeValue(recordJson));
    when(instance.getHrid()).thenReturn(targetInstanceHrid);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture());
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));

    doReturn(Future.succeededFuture(instanceId)).when(instanceOperationsHelper).updateInstance(any(), any());

    // when
    Future<String> future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().equals(instanceId));

      ArgumentCaptor<Instance> updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper, times(1)).updateInstance(updatedInstanceCaptor.capture(), any());
      Instance updatedInstance = updatedInstanceCaptor.getValue();
      context.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      context.assertEquals(targetInstanceHrid, updatedInstance.getHrid());

      async.complete();
    });

  }
  @Test
  public void getSourceMARCByInstanceIdSuccessTest() {

    String instanceId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String sourceTenant = "consortium";

    Record mockRecord = new Record();
    mockRecord.setId(instanceId);

    HttpResponse<Buffer> recordHttpResponse = mock(HttpResponse.class);

    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.succeededFuture(recordHttpResponse));

    when(recordHttpResponse.statusCode()).thenReturn(HttpStatus.HTTP_OK.toInt());
    when(recordHttpResponse.bodyAsString()).thenReturn("{\"id\":\"" + instanceId + "\"}");
    when(recordHttpResponse.bodyAsJson(Record.class)).thenReturn(mockRecord);

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, vertx);
    handler.getSourceMARCByInstanceId(instanceId, sourceTenant, sourceStorageClient).onComplete(result -> {
      Record record = result.result();
      assertEquals(instanceId, record.getId());
    });
  }

  @Test
  public void getSourceMARCByInstanceIdFailTest() {

    String instanceId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String sourceTenant = "sourceTenant";

    Record mockRecord = new Record();
    mockRecord.setId(instanceId);

    HttpResponse<Buffer> recordHttpResponse = mock(HttpResponse.class);

    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    when(recordHttpResponse.statusCode()).thenReturn(HttpStatus.HTTP_OK.toInt());
    when(recordHttpResponse.bodyAsString()).thenReturn("{\"id\":\"" + instanceId + "\"}");
    when(recordHttpResponse.bodyAsJson(Record.class)).thenReturn(mockRecord);

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, vertx);
    handler.getSourceMARCByInstanceId(instanceId, sourceTenant, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));
  }

  @Test
  public void deleteSourceRecordByInstanceIdSuccessTest() {

    String recordId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    String instanceId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String tenant = "sourceTenant";

    when(sourceStorageClient.deleteSourceStorageRecordsById(any()))
      .thenReturn(Future.succeededFuture());

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, vertx);
    handler.deleteSourceRecordByInstanceId(recordId, instanceId, tenant, sourceStorageClient)
      .onComplete(result -> assertEquals(instanceId, result.result()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId);
  }

  @Test
  public void deleteSourceRecordByInstanceIdFailedTest() {

    String instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    String recordId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String tenant = "sourceTenant";

    when(sourceStorageClient.deleteSourceStorageRecordsById(any()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, vertx);
    handler.deleteSourceRecordByInstanceId(recordId, instanceId, tenant, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId);
  }

  private static void setField(Object instance, String fieldName, Object fieldValue) {
    try {
      Field field = instance.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(instance, fieldValue);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to set field value using reflection", e);
    }
  }

  private final static String recordJson = "{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"snapshotId\":\"7376bb73-845e-44ce-ade4-53394f7526a6\",\"matchedId\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\",\"generation\":1,\"recordType\":\"MARC_BIB\"," +
    "\"parsedRecord\":{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"content\":{\"fields\":[{\"001\":\"in00000000001\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"  2006214613\"}]}},{\"019\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"1058285745\"}]}},{\"022\":{\"ind1\":\"0\",\"ind2\":\" \"," +
    "\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}]}},{\"035\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}]}},{\"040\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"ILGA\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"ISSN RECORD\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"4\",\"subfields\":[{\"a\":\"QL640\"}]}},{\"082\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}]}},{\"130\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}]}},{\"210\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}]}},{\"222\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}]}},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Biology!!!!!\"}]}},{\"246\":{\"ind1\":\"1\",\"ind2\":\"3\",\"subfields\":[{\"a\":\"HCBBBB\"}]}},{\"260\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}]}},{\"310\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Semiannual\"}]}},{\"336\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}]}},{\"337\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}]}},{\"338\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}]}},{\"362\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"655\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Electronic journals.\"}]}},{\"710\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}]}},{\"711\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}]}},{\"776\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}]}},{\"841\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"v.1- (1992-)\"}]}},{\"856\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\"}]}}],\"leader\":\"01819cas a2200469 a 4500\"},\"formattedContent\":\"LEADER 01819cas a2200469 a 4500\\n001 in00000000001\\n006 m     o  d        \\n007 cr cnu||||||||\\n008 060504c20069999txufr pso     0   a0eng c\\n005 20230915131710.4\\n010   $a  2006214613\\n019   $a1058285745\\n022 0 $a1931-7603$l1931-7603$21\\n035   $a(OCoLC)68188263$z(OCoLC)1058285745\\n040   $aNSD$beng$cNSD$dWAU$dDLC$dHUL$dOCLCQ$dOCLCF$dOCL$dAU@\\n042   $apcc$ansdp\\n049   $aILGA\\n050 10$aISSN RECORD\\n050 14$aQL640\\n082 10$a598.1$214\\n130 0 $aHerpetological conservation and biology (Online)\\n210 0 $aHerpetol. conserv. biol.$b(Online)\\n222  0$aHerpetological conservation and biology$b(Online)\\n245 10$aBiology!!!!!\\n246 13$aHCBBBB\\n260   $a[Texarkana, Tex.] :$b[publisher not identified],$c[2006]\\n310   $aSemiannual\\n336   $atext$btxt$2rdacontent\\n337   $acomputer$bc$2rdamedia\\n338   $aonline resource$bcr$2rdacarrier\\n362 1 $aBegan with: Vol. 1, issue 1 (Sept. 2006).\\n500   $aPublished in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\\n650  0$aHerpetology$xConservation$vPeriodicals.\\n650  0$aAmphibians$xConservation$vPeriodicals.\\n650  0$aReptiles$xConservation$vPeriodicals.\\n655  0$aElectronic journals.\\n710 2 $aPartners in Amphibian and Reptile Conservation.\\n711 2 $aWorld Congress of Herpetology.\\n776 1 $tHerpetological conservation and biology (Print)$x2151-0733$w(DLC)  2009202029$w(OCoLC)427887140\\n841   $av.1- (1992-)\\n856 40$uhttp://www.herpconbio.org$zAvailable to Lehigh users\\n999 ff$s0ecd6e9f-f02f-47b7-8326-2743bfa3fc43$ifea6477b-d8f5-4d22-9e86-6218407c780b\\n\\n\"}," +
    "\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\",\"instanceHrid\":\"in00000000001\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\",\"metadata\":{\"createdDate\":\"2023-09-15T13:17:10.571+00:00\",\"updatedDate\":\"2023-09-15T13:17:10.571+00:00\"}}";

}
