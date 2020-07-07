package api.support;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import api.ApiTestSuite;
import api.support.fixtures.InstanceRelationshipTypeFixture;
import api.support.fixtures.MarkItemMissingFixture;
import api.support.fixtures.MarkItemWithdrawnFixture;
import api.support.http.ResourceClient;

public abstract class ApiTests {
  private static boolean runningOnOwn;
  protected static OkapiHttpClient okapiClient;

  protected final ResourceClient holdingsStorageClient;
  protected final ResourceClient itemsStorageClient;
  protected final ResourceClient itemsClient;
  protected final ResourceClient instancesClient;
  protected final ResourceClient instancesStorageClient;
  protected final ResourceClient isbnClient;
  protected final ResourceClient usersClient;
  protected final ResourceClient instancesBatchClient;
  protected final ResourceClient precedingSucceedingTitlesClient;
  protected final ResourceClient instanceRelationshipClient;
  protected final ResourceClient requestStorageClient;

  protected final InstanceRelationshipTypeFixture instanceRelationshipTypeFixture;
  protected final MarkItemWithdrawnFixture markWithdrawnFixture;
  protected final MarkItemMissingFixture markMissingFixture;

  public ApiTests() {
    holdingsStorageClient = ResourceClient.forHoldingsStorage(okapiClient);
    itemsStorageClient = ResourceClient.forItemsStorage(okapiClient);
    itemsClient = ResourceClient.forItems(okapiClient);
    instancesClient = ResourceClient.forInstances(okapiClient);
    instancesStorageClient = ResourceClient.forInstancesStorage(okapiClient);
    isbnClient = ResourceClient.forIsbns(okapiClient);
    usersClient = ResourceClient.forUsers(okapiClient);
    instancesBatchClient = ResourceClient.forInstancesBatch(okapiClient);
    precedingSucceedingTitlesClient = ResourceClient.forPrecedingSucceedingTitles(okapiClient);
    instanceRelationshipClient = ResourceClient.forInstanceRelationship(okapiClient);
    requestStorageClient = ResourceClient.forRequestStorage(okapiClient);
    instanceRelationshipTypeFixture = new InstanceRelationshipTypeFixture(okapiClient);
    markWithdrawnFixture = new MarkItemWithdrawnFixture(okapiClient);
    markMissingFixture = new MarkItemMissingFixture(okapiClient);
  }

  @BeforeClass
  public static void before()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

    if(ApiTestSuite.isNotInitialised()) {
      System.out.println("Running test on own, initialising suite manually");
      runningOnOwn = true;
      ApiTestSuite.before();
    }

    okapiClient = ApiTestSuite.createOkapiHttpClient();
  }

  @AfterClass
  public static void after()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    if(runningOnOwn) {
      System.out.println("Running test on own, un-initialising suite manually");
      ApiTestSuite.after();
    }
  }

  @Before
  public void setup()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    Preparation preparation = new Preparation(okapiClient);
    preparation.deleteItems();
    holdingsStorageClient.deleteAll();
    preparation.deleteInstances();

    precedingSucceedingTitlesClient.deleteAll();
    instanceRelationshipClient.deleteAll();
  }
}
