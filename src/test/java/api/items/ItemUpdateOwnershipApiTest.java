package api.items;

import api.support.ApiTests;
import junitparams.JUnitParamsRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import static api.ApiTestSuite.createConsortiumTenant;

@Ignore
@RunWith(JUnitParamsRunner.class)
public class ItemUpdateOwnershipApiTest extends ApiTests {
  private static final String INSTANCE_ID = "instanceId";

  @Before
  public void initConsortia() throws Exception {
    createConsortiumTenant();
  }

  @After
  public void clearConsortia() throws Exception {
    userTenantsClient.deleteAll();
  }
}
