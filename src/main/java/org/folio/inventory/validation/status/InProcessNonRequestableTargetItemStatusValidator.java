package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class InProcessNonRequestableTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public InProcessNonRequestableTargetItemStatusValidator() {
    super(ItemStatusName.IN_PROCESS_NON_REQUESTABLE, of(
      ItemStatusName.AGED_TO_LOST,
      ItemStatusName.AVAILABLE,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.CHECKED_OUT,
      ItemStatusName.CLAIMED_RETURNED,
      ItemStatusName.DECLARED_LOST,
      ItemStatusName.INTELLECTUAL_ITEM,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.LONG_MISSING,
      ItemStatusName.LOST_AND_PAID,
      ItemStatusName.MISSING,
      ItemStatusName.ON_ORDER,
      ItemStatusName.ORDER_CLOSED,
      ItemStatusName.PAGED,
      ItemStatusName.RESTRICTED,
      ItemStatusName.UNAVAILABLE,
      ItemStatusName.UNKNOWN,
      ItemStatusName.WITHDRAWN
    ));
  }
}
