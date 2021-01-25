package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class InProcessNonRequestableTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public InProcessNonRequestableTargetItemStatusValidator() {
    super(ItemStatusName.IN_PROCESS_NON_REQUESTABLE, of(
      ItemStatusName.AVAILABLE,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.LOST_AND_PAID,
      ItemStatusName.MISSING,
      ItemStatusName.ORDER_CLOSED,
      ItemStatusName.PAGED,
      ItemStatusName.WITHDRAWN
    ));
  }
}
