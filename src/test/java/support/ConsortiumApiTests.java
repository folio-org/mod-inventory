package support;

import static api.ApiTestSuite.createConsortiumTenant;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Common consortium setup/teardown shared by tests that exercise the update-ownership and
 * move APIs across a central (consortium) tenant, a member tenant and, in some cases, a
 * college tenant.
 */
public abstract class ConsortiumApiTests extends ApiTests {

  @BeforeEach
  void initConsortia() {
    createConsortiumTenant();
  }

  @AfterEach
  void clearConsortia() throws Exception {
    userTenantsClient.deleteAll();
  }

  /**
   * Seeds the same instance, marked as shared, across the central (consortium) tenant and
   * the member (local) tenant - the common pattern used by the update-ownership tests to set
   * up an instance that both tenants can see and operate on.
   *
   * <p>This only covers the shared-instance case: the central tenant gets
   * {@code CONSORTIUM_FOLIO} and the member tenant gets {@code FOLIO}. It deliberately does
   * not cover the college tenant (some tests additionally seed the college tenant with
   * {@code CONSORTIUM_FOLIO} to extend sharing to a third tenant, and at least one test
   * deliberately seeds the college tenant with {@code FOLIO} - i.e. NOT shared - to exercise
   * the "instance not shared" rejection path). Callers needing either of those variants
   * should call this helper and then issue the additional/college-specific
   * {@code InstanceApiClient.createInstance} call themselves.
   *
   * @param instance the instance body to seed (as produced by e.g.
   *                 {@code InstanceFixture.smallAngryPlanet}); must already have an "id"
   * @return the instance's id, extracted from the "id" property of {@code instance}
   */
  protected UUID createSharedInstanceAcrossTenants(JsonObject instance) {
    UUID instanceId = UUID.fromString(instance.getString("id"));

    InstanceApiClient.createInstance(okapiClient, instance.copy().put("source", CONSORTIUM_FOLIO.getValue()));
    InstanceApiClient.createInstance(consortiumOkapiClient, instance.copy().put("source", FOLIO.getValue()));

    return instanceId;
  }
}
