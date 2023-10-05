package org.folio.inventory.consortium.services;

import io.vertx.core.Future;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.inventory.common.Context;

import java.util.List;

public interface EntitiesLinksService {
  Future<List<Link>> getInstanceAuthorityLinks(Context context, String instanceId);
  Future<List<Link>> putInstanceAuthorityLinks(Context context, String instanceId, List<Link> entityLinks);
  Future<List<LinkingRuleDto>> getLinkingRules(Context context);
}
