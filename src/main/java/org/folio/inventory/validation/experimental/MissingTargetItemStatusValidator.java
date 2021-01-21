package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class MissingTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public MissingTargetItemStatusValidator() {
    super(ItemStatusName.MISSING, of(
      ItemStatusName.AVAILABLE,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.IN_PROCESS,
      ItemStatusName.PAGED,
      ItemStatusName.WITHDRAWN
    ));
  }
}
