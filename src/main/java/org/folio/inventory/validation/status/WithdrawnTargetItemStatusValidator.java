package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class WithdrawnTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public WithdrawnTargetItemStatusValidator() {
    super(ItemStatusName.WITHDRAWN, of(
      ItemStatusName.AVAILABLE,
      ItemStatusName.LOST_AND_PAID,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.IN_PROCESS,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.MISSING,
      ItemStatusName.PAGED));
  }
}
