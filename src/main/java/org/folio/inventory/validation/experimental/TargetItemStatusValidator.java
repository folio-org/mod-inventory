package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.ItemStatusName;

import java.util.EnumMap;

public class TargetItemStatusValidator {
  private static final EnumMap<ItemStatusName, AbstractTargetValidator> validators = new EnumMap<>(ItemStatusName.class);

  public TargetItemStatusValidator() {
    validators.put(ItemStatusName.IN_PROCESS, new InProcessTargetValidator());
    validators.put(ItemStatusName.IN_PROCESS_NON_REQUESTABLE, new InProcessNonRequestableTargetValidator());
    validators.put(ItemStatusName.INTELLECTUAL_ITEM, new IntellectualItemTargetValidator());
  }

  public AbstractTargetValidator getValidator(ItemStatusName itemStatusName) {
    return validators.get(itemStatusName);
  }

}
