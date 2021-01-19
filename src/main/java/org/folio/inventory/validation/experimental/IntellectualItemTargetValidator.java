package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class IntellectualItemTargetValidator extends AbstractTargetValidator {
  public IntellectualItemTargetValidator() {
    super(ItemStatusName.INTELLECTUAL_ITEM, of(
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
