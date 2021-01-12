package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.ItemStatusName;

import java.util.EnumMap;

public class TargetItemStatusValidator {
  private static final EnumMap<ItemStatusName, TargetItemStatusValidatorInterface> validators = new EnumMap<> (ItemStatusName.class);

  public TargetItemStatusValidator() {
    validators.put(InProcessTargetValidator.statusName,new InProcessTargetValidator());
    validators.put(InProcessNonRequestableTargetValidator.statusName,new InProcessNonRequestableTargetValidator());
  }

  public TargetItemStatusValidatorInterface getValidator(ItemStatusName itemStatusName) {
    return validators.get(itemStatusName);
  }

}
