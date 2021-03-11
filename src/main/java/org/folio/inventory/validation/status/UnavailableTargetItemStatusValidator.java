package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class UnavailableTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public UnavailableTargetItemStatusValidator() {
    super(ItemStatusName.UNAVAILABLE, of(
      ItemStatusName.AGED_TO_LOST,
      ItemStatusName.CHECKED_OUT,
      ItemStatusName.CLAIMED_RETURNED,
      ItemStatusName.DECLARED_LOST,
      ItemStatusName.AVAILABLE,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.INTELLECTUAL_ITEM,
      ItemStatusName.IN_PROCESS_NON_REQUESTABLE,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.LONG_MISSING,
      ItemStatusName.LOST_AND_PAID,
      ItemStatusName.MISSING,
      ItemStatusName.ON_ORDER,
      ItemStatusName.ORDER_CLOSED,
      ItemStatusName.RESTRICTED,
      ItemStatusName.PAGED,
      ItemStatusName.UNKNOWN,
      ItemStatusName.WITHDRAWN));
  }
}
