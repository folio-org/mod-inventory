package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.ItemStatusName;
import static java.util.Set.of;

public class InProcessNonRequestableTargetValidator extends AbstractTargetValidator {
  public InProcessNonRequestableTargetValidator() {
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
