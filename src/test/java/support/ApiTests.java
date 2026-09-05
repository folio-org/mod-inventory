package support;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import api.ApiTestSuite;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.storage.external.AbstractExternalStorageTest;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.util.PercentCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import support.fixtures.InstanceRelationshipTypeFixture;
import support.fixtures.MarkItemFixture;
import support.http.ResourceClient;

public abstract class ApiTests {

  protected static OkapiHttpClient okapiClient;
  protected static OkapiHttpClient consortiumOkapiClient;
  protected static OkapiHttpClient collegeOkapiClient;

  private static final Logger LOGGER = LogManager.getLogger(AbstractExternalStorageTest.class);
  private static boolean runningOnOwn;

  protected final ResourceClient holdingsStorageClient;
  protected final ResourceClient holdingsSourceStorageClient;
  protected final ResourceClient itemsStorageClient;
  protected final ResourceClient itemsClient;
  protected final ResourceClient instancesClient;
  protected final ResourceClient instancesStorageClient;
  protected final ResourceClient isbnClient;
  protected final ResourceClient usersClient;
  protected final ResourceClient userTenantsClient;
  protected final ResourceClient instancesBatchClient;
  protected final ResourceClient precedingSucceedingTitlesClient;
  protected final ResourceClient instanceRelationshipClient;
  protected final ResourceClient requestStorageClient;
  protected final ResourceClient sourceRecordStorageClient;
  protected final ResourceClient consortiumItemsClient;
  protected final ResourceClient consortiumHoldingsStorageClient;
  protected final ResourceClient collegeItemsClient;
  protected final ResourceClient collegeHoldingsStorageClient;
  protected final ResourceClient boundWithPartsStorageClient;
  protected final InstanceRelationshipTypeFixture instanceRelationshipTypeFixture;
  protected final MarkItemFixture markItemFixture;
  protected final ResourceClient collegeSourceRecordStorageClient;

  public ApiTests() {
    holdingsStorageClient = ResourceClient.forHoldingsStorage(okapiClient);
    holdingsSourceStorageClient = ResourceClient.forHoldingSourceRecord(okapiClient);
    itemsStorageClient = ResourceClient.forItemsStorage(okapiClient);
    itemsClient = ResourceClient.forItems(okapiClient);
    instancesClient = ResourceClient.forInstances(okapiClient);
    instancesStorageClient = ResourceClient.forInstancesStorage(okapiClient);
    isbnClient = ResourceClient.forIsbns(okapiClient);
    usersClient = ResourceClient.forUsers(okapiClient);
    userTenantsClient = ResourceClient.forUserTenants(okapiClient);
    instancesBatchClient = ResourceClient.forInstancesBatch(okapiClient);
    precedingSucceedingTitlesClient = ResourceClient.forPrecedingSucceedingTitles(okapiClient);
    instanceRelationshipClient = ResourceClient.forInstanceRelationship(okapiClient);
    requestStorageClient = ResourceClient.forRequestStorage(okapiClient);
    sourceRecordStorageClient = ResourceClient.forSourceRecordStorage(okapiClient);
    boundWithPartsStorageClient = ResourceClient.forBoundWithPartsStorage(okapiClient);
    instanceRelationshipTypeFixture = new InstanceRelationshipTypeFixture(okapiClient);
    markItemFixture = new MarkItemFixture(okapiClient);

    consortiumHoldingsStorageClient = ResourceClient.forHoldingsStorage(consortiumOkapiClient);
    consortiumItemsClient = ResourceClient.forItemsStorage(consortiumOkapiClient);

    collegeHoldingsStorageClient = ResourceClient.forHoldingsStorage(collegeOkapiClient);
    collegeItemsClient = ResourceClient.forItemsStorage(collegeOkapiClient);
    collegeSourceRecordStorageClient = ResourceClient.forSourceRecordStorage(collegeOkapiClient);
  }

  @BeforeAll
  public static void before() {
    if (ApiTestSuite.isNotInitialised()) {
      LOGGER.info("Running test on own, initialising suite manually");
      runningOnOwn = true;
      ApiTestSuite.before();
    }

    okapiClient = ApiTestSuite.createOkapiHttpClient();
    consortiumOkapiClient = ApiTestSuite.createOkapiHttpClient(ApiTestSuite.CONSORTIA_TENANT_ID);
    collegeOkapiClient = ApiTestSuite.createOkapiHttpClient(ApiTestSuite.COLLEGE_TENANT_ID);
  }

  @AfterAll
  public static void after() {
    if (runningOnOwn) {
      LOGGER.info("Running test on own, un-initialising suite manually");
      ApiTestSuite.after();
    }
  }

  @BeforeEach
  public void setup()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    deleteItems();
    holdingsStorageClient.deleteAll();
    deleteInstances();

    precedingSucceedingTitlesClient.deleteAll();
    instanceRelationshipClient.deleteAll();
  }

  public void deleteInstances() {
    deleteAll(ApiRoot.instances());
  }

  public void deleteItems() {
    deleteAll(ApiRoot.items());
  }

  @SneakyThrows
  private void deleteAll(URL root) {
    var deleteCompleted = okapiClient.delete(root + "?query=" + PercentCodec.encode("cql.allRecords=1"));
    Response response = deleteCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat("Failed to delete all records", response.statusCode(), is(204));
  }
}
