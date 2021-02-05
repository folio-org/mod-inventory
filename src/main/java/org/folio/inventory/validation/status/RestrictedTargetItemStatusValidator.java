package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class RestrictedTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public RestrictedTargetItemStatusValidator() {
    super(ItemStatusName.RESTRICTED, of(
      ItemStatusName.AGED_TO_LOST,
      ItemStatusName.CLAIMED_RETURNED,
      ItemStatusName.CHECKED_OUT,
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
      ItemStatusName.PAGED,
      ItemStatusName.UNAVAILABLE,
      ItemStatusName.UNKNOWN,
      ItemStatusName.WITHDRAWN));
  }
}
