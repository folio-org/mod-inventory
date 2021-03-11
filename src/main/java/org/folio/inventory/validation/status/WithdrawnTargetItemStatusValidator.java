package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class WithdrawnTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public WithdrawnTargetItemStatusValidator() {
    super(ItemStatusName.WITHDRAWN, of(
      ItemStatusName.AVAILABLE,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.LOST_AND_PAID,
      ItemStatusName.IN_PROCESS,
      ItemStatusName.INTELLECTUAL_ITEM,
      ItemStatusName.IN_PROCESS_NON_REQUESTABLE,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.LONG_MISSING,
      ItemStatusName.MISSING,
      ItemStatusName.PAGED,
      ItemStatusName.RESTRICTED,
      ItemStatusName.UNAVAILABLE,
      ItemStatusName.UNKNOWN));
  }
}
