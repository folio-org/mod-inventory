package api.support;

import api.ApiTestSuite;
import api.support.http.ResourceClient;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class ApiTests {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static boolean runningOnOwn;
  protected final OkapiHttpClient okapiClient;

  protected final ResourceClient holdingsStorageClient;
  protected final ResourceClient itemsClient;
  protected final ResourceClient instancesClient;

  public ApiTests() throws MalformedURLException {
    okapiClient = ApiTestSuite.createOkapiHttpClient();
    holdingsStorageClient = ResourceClient.forHoldingsStorage(okapiClient);
    itemsClient = ResourceClient.forItems(okapiClient);
    instancesClient = ResourceClient.forInstances(okapiClient);
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
  }

  @AfterClass
  public static void after()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

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
  }
}
