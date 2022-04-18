package org.folio.inventory.dataimport.handlers.matching.preloaders;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum PreloadingFields {
    POL("purchaseOrderLineNumber");

    private final String existingMatchField;
}
