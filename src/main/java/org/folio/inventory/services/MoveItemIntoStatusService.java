package org.folio.inventory.services;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.domain.items.ItemStatusName.INTELLECTUAL_ITEM;
import static org.folio.inventory.domain.items.ItemStatusName.IN_PROCESS;
import static org.folio.inventory.domain.items.ItemStatusName.IN_PROCESS_NON_REQUESTABLE;
import static org.folio.inventory.domain.items.ItemStatusName.LONG_MISSING;
import static org.folio.inventory.domain.items.ItemStatusName.MISSING;
import static org.folio.inventory.domain.items.ItemStatusName.RESTRICTED;
import static org.folio.inventory.domain.items.ItemStatusName.UNAVAILABLE;
import static org.folio.inventory.domain.items.ItemStatusName.UNKNOWN;
import static org.folio.inventory.domain.items.ItemStatusName.WITHDRAWN;
import static org.folio.inventory.domain.view.request.RequestStatus.OPEN_NOT_YET_FILLED;

import java.util.concurrent.CompletableFuture;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.view.request.Request;
import org.folio.inventory.storage.external.Clients;
import org.folio.inventory.storage.external.repository.RequestRepository;
import org.folio.inventory.validation.MarkAsInProcessNonRequestableValidators;
import org.folio.inventory.validation.MarkAsIntellectualItemValidators;
import org.folio.inventory.validation.MarkAsLongMissingValidators;
import org.folio.inventory.validation.MarkAsMissingValidators;
import org.folio.inventory.validation.MarkAsRestrictedValidators;
import org.folio.inventory.validation.MarkAsUnavailableValidators;
import org.folio.inventory.validation.MarkAsUnknownValidators;
import org.folio.inventory.validation.MarkAsWithdrawnValidators;
import org.folio.inventory.validation.ItemsValidator;
import org.folio.inventory.validation.experimental.AbstractTargetItemStatusValidator;
import org.folio.inventory.validation.experimental.TargetItemStatusValidator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoveItemIntoStatusService {
  private static final Logger log = LoggerFactory.getLogger(MoveItemIntoStatusService.class);

  private final ItemCollection itemCollection;
  private final RequestRepository requestRepository;

  private static final TargetItemStatusValidator validator = new TargetItemStatusValidator();

  public MoveItemIntoStatusService(ItemCollection itemCollection, Clients clients) {
    this.itemCollection = itemCollection;
    this.requestRepository = new RequestRepository(clients);
  }

  public CompletableFuture<Item> processMarkItemWithdrawn(WebContext context) {
    final String itemId = context.getStringParameter("id", null);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(MarkAsWithdrawnValidators::itemHasAllowedStatusToMarkAsWithdrawn)
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(WITHDRAWN))
      .thenCompose(itemCollection::update);
  }

  public CompletableFuture<Item> processMarkItem(WebContext context,ItemStatusName statusName) {
    final String itemId = context.getStringParameter("id", null);
    AbstractTargetItemStatusValidator targetValidator = validator.getValidator(statusName);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(item -> targetValidator.itemHasAllowedStatusToMark(item))
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(statusName))
      .thenCompose(itemCollection::update);
  }

//  public CompletableFuture<Item> processMarkItemInProcess(WebContext context) {
//    final String itemId = context.getStringParameter("id", null);
//
//    return itemCollection.findById(itemId)
//      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
//      .thenCompose(MarkAsInProcessValidators::itemHasAllowedStatusToMarkAsInProcess)
//      .thenCompose(this::updateRequestStatusIfRequired)
//      .thenApply(item -> item.changeStatus(IN_PROCESS))
//      .thenCompose(itemCollection::update);
//  }

  public CompletableFuture<Item> processMarkItemInProcess(WebContext context) {
    final String itemId = context.getStringParameter("id", null);
    AbstractTargetItemStatusValidator targetValidator = validator.getValidator(IN_PROCESS);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(item -> targetValidator.itemHasAllowedStatusToMark(item))
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(targetValidator.getItemStatusName()))
      .thenCompose(itemCollection::update);
  }

  public CompletableFuture<Item> processMarkItemInProcessNonRequestable(WebContext context) {
    final String itemId = context.getStringParameter("id", null);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(MarkAsInProcessNonRequestableValidators::itemHasAllowedStatusToMarkAsInProcessNonRequestable)
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(IN_PROCESS_NON_REQUESTABLE))
      .thenCompose(itemCollection::update);
  }

  public CompletableFuture<Item> processMarkItemIntellectualItem(WebContext context) {
    final String itemId = context.getStringParameter("id", null);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(MarkAsIntellectualItemValidators::itemHasAllowedStatusToMarkAsIntellectualItem)
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(INTELLECTUAL_ITEM))
      .thenCompose(itemCollection::update);
  }

  public CompletableFuture<Item> processMarkItemLongMissing(WebContext context) {
    final String itemId = context.getStringParameter("id", null);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(MarkAsLongMissingValidators::itemHasAllowedStatusToMarkAsLongMissing)
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(LONG_MISSING))
      .thenCompose(itemCollection::update);
  }

  public CompletableFuture<Item> processMarkItemMissing(WebContext context) {
    final String itemId = context.getStringParameter("id", null);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(MarkAsMissingValidators::itemHasAllowedStatusToMarkAsMissing)
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(MISSING))
      .thenCompose(itemCollection::update);
  }

  public CompletableFuture<Item> processMarkItemRestricted(WebContext context) {
    final String itemId = context.getStringParameter("id", null);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(MarkAsRestrictedValidators::itemHasAllowedStatusToMarkAsRestricted)
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(RESTRICTED))
      .thenCompose(itemCollection::update);
  }

  public CompletableFuture<Item> processMarkItemUnavailable(WebContext context) {
    final String itemId = context.getStringParameter("id", null);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(MarkAsUnavailableValidators::itemHasAllowedStatusToMarkAsUnavailable)
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(UNAVAILABLE))
      .thenCompose(itemCollection::update);
  }


  public CompletableFuture<Item> processMarkItemUnknown(WebContext context) {
    final String itemId = context.getStringParameter("id", null);

    return itemCollection.findById(itemId)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(MarkAsUnknownValidators::itemHasAllowedStatusToMarkAsUnknown)
      .thenCompose(this::updateRequestStatusIfRequired)
      .thenApply(item -> item.changeStatus(UNKNOWN))
      .thenCompose(itemCollection::update);
  }


  private CompletableFuture<Item> updateRequestStatusIfRequired(Item item) {
    return requestRepository.getRequestInFulfilmentForItem(item.id)
      .thenCompose(requestOptional -> {
        if (requestOptional.isEmpty() || requestIsExpiredOnHoldShelf(requestOptional.get())) {
          log.debug("No request in fulfillment or it is expired");
          return completedFuture(item);
        }

        log.debug("Found a request that is being fulfilled {}", requestOptional.get().getId());
        return moveRequestIntoNotYetFilledStatus(requestOptional.get())
          .thenApply(notUsed -> item);
      });
  }

  private boolean requestIsExpiredOnHoldShelf(Request request) {
    return request.getHoldShelfExpirationDate() != null
      && currentDateTime().isAfter(request.getHoldShelfExpirationDate());
  }

  private CompletableFuture<Request> moveRequestIntoNotYetFilledStatus(Request request) {
    request.setStatus(OPEN_NOT_YET_FILLED);

    return requestRepository.update(request);
  }

  private DateTime currentDateTime() {
    return DateTime.now(DateTimeZone.UTC);
  }
}
