package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;

import java.util.Set;
import static java.util.Set.of;

public class InProcessTargetValidator extends AbstractTargetValidator {
  public static final ItemStatusName statusName = ItemStatusName.IN_PROCESS;
  private static final Set<ItemStatusName> ALLOWED_STATUS_TO_MARK = of(
    ItemStatusName.AVAILABLE,
    ItemStatusName.AWAITING_DELIVERY,
    ItemStatusName.AWAITING_PICKUP,
    ItemStatusName.IN_TRANSIT,
    ItemStatusName.LOST_AND_PAID,
    ItemStatusName.MISSING,
    ItemStatusName.ORDER_CLOSED,
    ItemStatusName.PAGED,
    ItemStatusName.WITHDRAWN
  );

  @Override
  public ItemStatusName getStatusName() {
    return statusName;
  }

  @Override
  public Boolean isItemAllowedToMark(Item item) {
    return ALLOWED_STATUS_TO_MARK.contains(item.getStatus().getName());
  }

  @Override
  public Set<ItemStatusName> getAllStatusesAllowedToMark() {
    return ALLOWED_STATUS_TO_MARK;
  }
}
