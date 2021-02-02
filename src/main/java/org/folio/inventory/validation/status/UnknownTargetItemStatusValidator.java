package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class UnknownTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public UnknownTargetItemStatusValidator() {
    super(ItemStatusName.UNKNOWN, of(
      ItemStatusName.AVAILABLE,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.INTELLECTUAL_ITEM,
      ItemStatusName.IN_PROCESS_NON_REQUESTABLE,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.LONG_MISSING,
      ItemStatusName.LOST_AND_PAID,
      ItemStatusName.MISSING,
      ItemStatusName.ORDER_CLOSED,
      ItemStatusName.PAGED,
      ItemStatusName.RESTRICTED,
      ItemStatusName.UNAVAILABLE,
      ItemStatusName.WITHDRAWN));
  }
}
