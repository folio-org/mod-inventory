package org.folio.inventory.consortium.services;

import io.vertx.core.Future;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.SharingInstance;

public interface ConsortiumService {
  /**
   * Retrieves centralTenantId, consortiumId and runs job at mod-consortia that creates shadow instance for locale tenant
   * @param context                    - Context for creating shadow instance.
   * @param instanceId                 - id of shadow instance to create.
   * @return - future of sharingInstance
   */
  Future<SharingInstance> createShadowInstance(Context context, String instanceId);
  /**
   * Retrieves centralTenantId from mod-users
   * @param context                    - Context for retrieving centralTenantId.
   * @return - future of centralTenantId
   */
  Future<String> getCentralTenantId(Context context);
  /**
   * Retrieves consortiumId from mod-consortia
   * @param context                    - Context for retrieving consortiumId.
   * @return - future of consortiumId
   */
  Future<String> getConsortiumId(Context context);
  /**
   * Starts sharing instance process
   * @param context                    - Context for running sharing process.
   * @param consortiumId               - Consortium id for running sharing process.
   * @param sharingInstance            - Sharing Instance entity that configures sourceTenantId, targetTenantId and instanceIdentifier.
   * @return - future of sharingInstance
   */
  Future<SharingInstance> shareInstance(Context context, String consortiumId, SharingInstance sharingInstance);
}
