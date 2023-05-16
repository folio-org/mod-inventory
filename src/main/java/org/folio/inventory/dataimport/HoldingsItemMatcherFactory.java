package org.folio.inventory.dataimport;

import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.matcher.HoldingsItemMatcher;
import org.folio.processing.matching.matcher.Matcher;
import org.folio.processing.matching.matcher.MatcherFactory;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.rest.jaxrs.model.EntityType;

public class HoldingsItemMatcherFactory implements MatcherFactory {
  @Override
  public Matcher createMatcher(MatchValueReader matchValueReader, MatchValueLoader matchValueLoader) {
    return new HoldingsItemMatcher(matchValueReader, matchValueLoader);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return EntityType.HOLDINGS == entityType || EntityType.ITEM == entityType;
  }
}
