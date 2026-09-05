package org.folio.inventory.common.domain;

import java.util.List;

public record MultipleRecords<T>(List<T> records, Integer totalRecords) {
}
