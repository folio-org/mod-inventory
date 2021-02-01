package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class IntellectualItemTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public IntellectualItemTargetItemStatusValidator() {
    super(ItemStatusName.INTELLECTUAL_ITEM, of(
      ItemStatusName.AVAILABLE,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.IN_PROCESS_NON_REQUESTABLE,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.LONG_MISSING,
      ItemStatusName.LOST_AND_PAID,
      ItemStatusName.MISSING,
      ItemStatusName.ORDER_CLOSED,
      ItemStatusName.PAGED,
      ItemStatusName.UNAVAILABLE,
      ItemStatusName.WITHDRAWN
    ));
  }
}
