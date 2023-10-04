package org.folio.inventory.consortium.services;

import io.vertx.core.Future;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.EntityLink;

import java.util.List;

public interface EntitiesLinksService {
  Future<List<EntityLink>> getInstanceAuthorityLinks(Context context, String instanceId);
  Future<List<EntityLink>> putInstanceAuthorityLinks(Context context, String instanceId, List<EntityLink> entityLinks);
}
