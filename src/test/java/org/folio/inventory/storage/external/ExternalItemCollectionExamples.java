package org.folio.inventory.storage.external;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static org.folio.inventory.common.FutureAssistance.fail;
import static org.folio.inventory.common.FutureAssistance.getOnCompletion;
import static org.folio.inventory.common.FutureAssistance.succeed;
import static org.folio.inventory.common.FutureAssistance.waitForCompletion;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WaitForAllFutures;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.junit.Before;
import org.junit.Test;

import lombok.SneakyThrows;

public class ExternalItemCollectionExamples extends ExternalStorageTests {
  private final String bookMaterialTypeId = UUID.randomUUID().toString();
  private final String canCirculateLoanTypeId = UUID.randomUUID().toString();
  private final String annexLibraryLocationId = UUID.randomUUID().toString();

  private final ItemCollection collection = useHttpClient(
    client -> new ExternalStorageModuleItemCollection(getStorageAddress(),
      TENANT_ID, TENANT_TOKEN, USER_ID, REQUEST_ID, client));

  private final Item smallAngryPlanet = smallAngryPlanet();
  private final Item nod = nod();
  private final Item uprooted = uprooted();
  private final Item temeraire = temeraire();
  private final Item interestingTimes = interestingTimes();

  @Before
  @SneakyThrows
  public void before() {
    CompletableFuture<Void> emptied = new CompletableFuture<>();

    collection.empty(succeed(emptied), fail(emptied));

    waitForCompletion(emptied);
  }

  @Test
  @SneakyThrows
  public void canBeEmptied() {
    addSomeExamples(collection);

    CompletableFuture<Void> emptied = new CompletableFuture<>();

    collection.empty(succeed(emptied), fail(emptied));

    waitForCompletion(emptied);

    CompletableFuture<MultipleRecords<Item>> findFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      succeed(findFuture), fail(findFuture));

    MultipleRecords<Item> wrappedItems = getOnCompletion(findFuture);

    List<Item> allItems = wrappedItems.records;

    assertThat(allItems.size(), is(0));
    assertThat(wrappedItems.totalRecords, is(0));
  }

  @Test
  @SneakyThrows
  public void anItemCanBeAdded() {
    addSomeExamples(collection);

    CompletableFuture<MultipleRecords<Item>> findFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(), succeed(findFuture),
      fail(findFuture));

    MultipleRecords<Item> wrappedItems = getOnCompletion(findFuture);

    List<Item> allItems = wrappedItems.records;

    assertThat(allItems.size(), is(3));
    assertThat(wrappedItems.totalRecords, is(3));

    Item smallAngry = getItem(allItems, "036000291452");

    assertThat(smallAngry, notNullValue());
    assertThat(smallAngry.getStatus().getName(), is(ItemStatusName.AVAILABLE));
    assertThat(smallAngry.getMaterialTypeId(), is(bookMaterialTypeId));
    assertThat(smallAngry.getPermanentLoanTypeId(), is(canCirculateLoanTypeId));
    assertThat(smallAngry.getTemporaryLocationId(), is(annexLibraryLocationId));

    Item nod = getItem(allItems, "565578437802");

    assertThat(nod, notNullValue());
    assertThat(nod.getStatus().getName(), is(ItemStatusName.AVAILABLE));
    assertThat(nod.getMaterialTypeId(), is(bookMaterialTypeId));
    assertThat(nod.getPermanentLoanTypeId(), is(canCirculateLoanTypeId));
    assertThat(nod.getTemporaryLocationId(), is(annexLibraryLocationId));

    Item uprooted = getItem(allItems, "657670342075");

    assertThat(uprooted, notNullValue());
    assertThat(uprooted.getStatus().getName(), is(ItemStatusName.AVAILABLE));
    assertThat(uprooted.getMaterialTypeId(), is(bookMaterialTypeId));
    assertThat(uprooted.getPermanentLoanTypeId(), is(canCirculateLoanTypeId));
    assertThat(uprooted.getTemporaryLocationId(), is(annexLibraryLocationId));
  }

  @Test
  @SneakyThrows
  public void anItemCanBeAddedWithAnId() {
    CompletableFuture<Item> addFinished = new CompletableFuture<>();

    String itemId = UUID.randomUUID().toString();

    Item itemWithId = smallAngryPlanet.copyWithNewId(itemId);

    collection.add(itemWithId, succeed(addFinished), fail(addFinished));

    Item added = getOnCompletion(addFinished);

    assertThat(added.id, is(itemId));
  }

  @Test
  @SneakyThrows
  public void anItemCanBeUpdated() {
    CompletableFuture<Item> addFinished = new CompletableFuture<>();

    collection.add(smallAngryPlanet, succeed(addFinished), fail(addFinished));

    Item added = getOnCompletion(addFinished);

    CompletableFuture<Void> updateFinished = new CompletableFuture<>();

    Item changed = added.changeStatus(ItemStatusName.CHECKED_OUT);

    collection.update(changed, succeed(updateFinished),
      fail(updateFinished));

    waitForCompletion(updateFinished);

    CompletableFuture<Item> gotUpdated = new CompletableFuture<>();

    collection.findById(added.id, succeed(gotUpdated),
      fail(gotUpdated));

    Item updated = getOnCompletion(gotUpdated);

    assertThat(updated.id, is(added.id));
    assertThat(updated.getBarcode(), is(added.getBarcode()));
    assertThat(updated.getTemporaryLocationId(), is(added.getTemporaryLocationId()));
    assertThat(updated.getMaterialTypeId(), is(added.getMaterialTypeId()));
    assertThat(updated.getPermanentLoanTypeId(), is(added.getPermanentLoanTypeId()));
    assertThat(updated.getStatus().getName(), is(ItemStatusName.CHECKED_OUT));
  }

  @Test
  @SneakyThrows
  public void anItemCanBeDeleted() {
    addSomeExamples(collection);

    CompletableFuture<Item> itemToBeDeletedFuture = new CompletableFuture<>();

    collection.add(temeraire(), succeed(itemToBeDeletedFuture),
      fail(itemToBeDeletedFuture));

    Item itemToBeDeleted = itemToBeDeletedFuture.get();

    CompletableFuture<Void> deleted = new CompletableFuture<>();

    collection.delete(itemToBeDeleted.id,
      succeed(deleted), fail(deleted));

    waitForCompletion(deleted);

    CompletableFuture<Item> findFuture = new CompletableFuture<>();

    collection.findById(itemToBeDeleted.id, succeed(findFuture),
      fail(findFuture));

    assertThat(findFuture.get(), is(nullValue()));

    CompletableFuture<MultipleRecords<Item>> findAllFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(), succeed(findAllFuture),
      fail(findAllFuture));

    MultipleRecords<Item> wrappedItems = getOnCompletion(findAllFuture);

    List<Item> allItems = wrappedItems.records;

    assertThat(allItems.size(), is(3));
    assertThat(wrappedItems.totalRecords, is(3));
  }

  @Test
  @SneakyThrows
  public void allItemsCanBePaged() {
    WaitForAllFutures<Item> allAdded = new WaitForAllFutures<>();

    collection.add(smallAngryPlanet, allAdded.notifySuccess(), v -> {});
    collection.add(nod, allAdded.notifySuccess(), v -> {});
    collection.add(uprooted, allAdded.notifySuccess(), v -> {});
    collection.add(temeraire, allAdded.notifySuccess(), v -> {});
    collection.add(interestingTimes, allAdded.notifySuccess(), v -> {});

    allAdded.waitForCompletion();

    CompletableFuture<MultipleRecords<Item>> firstPageFuture = new CompletableFuture<>();
    CompletableFuture<MultipleRecords<Item>> secondPageFuture = new CompletableFuture<>();

    collection.findAll(new PagingParameters(3, 0), succeed(firstPageFuture),
      fail(secondPageFuture));

    collection.findAll(new PagingParameters(3, 3), succeed(secondPageFuture),
      fail(secondPageFuture));

    MultipleRecords<Item> firstPage = getOnCompletion(firstPageFuture);
    MultipleRecords<Item> secondPage = getOnCompletion(secondPageFuture);

    assertThat(firstPage.records.size(), is(3));
    assertThat(secondPage.records.size(), is(2));

    assertThat(firstPage.totalRecords, is(5));
    assertThat(secondPage.totalRecords, is(5));
  }

  @Test
  @SneakyThrows
  public void itemsCanBeFoundByBarcode() {
    CompletableFuture<Item> firstAddFuture = new CompletableFuture<>();
    CompletableFuture<Item> secondAddFuture = new CompletableFuture<>();
    CompletableFuture<Item> thirdAddFuture = new CompletableFuture<>();

    collection.add(smallAngryPlanet, succeed(firstAddFuture),
      fail(firstAddFuture));
    collection.add(nod, succeed(secondAddFuture),
      fail(secondAddFuture));
    collection.add(uprooted, succeed(thirdAddFuture),
      fail(thirdAddFuture));

    CompletableFuture<Void> allAddsFuture = CompletableFuture.allOf(
      firstAddFuture, secondAddFuture, thirdAddFuture);

    getOnCompletion(allAddsFuture);

    Item addedSmallAngryPlanet = getOnCompletion(firstAddFuture);

    CompletableFuture<MultipleRecords<Item>> findFuture = new CompletableFuture<>();

    collection.findByCql("barcode==036000291452", new PagingParameters(10, 0),
      succeed(findFuture), fail(findFuture));

    MultipleRecords<Item> wrappedItems = getOnCompletion(findFuture);

    assertThat(wrappedItems.records.size(), is(1));
    assertThat(wrappedItems.totalRecords, is(1));

    assertThat(wrappedItems.records.stream().findFirst().get().id, is(addedSmallAngryPlanet.id));
  }

  @Test
  @SneakyThrows
  public void anItemCanBeFoundById() {
    CompletableFuture<Item> firstAddFuture = new CompletableFuture<>();
    CompletableFuture<Item> secondAddFuture = new CompletableFuture<>();

    collection.add(smallAngryPlanet, succeed(firstAddFuture),
      fail(firstAddFuture));

    collection.add(nod, succeed(secondAddFuture),
      fail(secondAddFuture));

    Item addedItem = getOnCompletion(firstAddFuture);
    Item otherAddedItem = getOnCompletion(secondAddFuture);

    CompletableFuture<Item> findFuture = new CompletableFuture<>();
    CompletableFuture<Item> otherFindFuture = new CompletableFuture<>();

    collection.findById(addedItem.id, succeed(findFuture),
      fail(findFuture));

    collection.findById(otherAddedItem.id, succeed(otherFindFuture),
      fail(otherFindFuture));

    Item foundItem = getOnCompletion(findFuture);
    Item otherFoundItem = getOnCompletion(otherFindFuture);

    assertThat(foundItem, notNullValue());
    assertThat(foundItem.getBarcode(), is("036000291452"));
    assertThat(foundItem.getStatus().getName(), is(ItemStatusName.AVAILABLE));
    assertThat(foundItem.getMaterialTypeId(), is(bookMaterialTypeId));
    assertThat(foundItem.getPermanentLoanTypeId(), is(canCirculateLoanTypeId));
    assertThat(foundItem.getTemporaryLocationId(), is(annexLibraryLocationId));

    assertThat(otherFoundItem, notNullValue());
    assertThat(otherFoundItem.getBarcode(), is("565578437802"));
    assertThat(otherFoundItem.getStatus().getName(), is(ItemStatusName.AVAILABLE));
    assertThat(otherFoundItem.getMaterialTypeId(), is(bookMaterialTypeId));
    assertThat(otherFoundItem.getPermanentLoanTypeId(), is(canCirculateLoanTypeId));
    assertThat(otherFoundItem.getTemporaryLocationId(), is(annexLibraryLocationId));
  }

  @SneakyThrows
  private void addSomeExamples(ItemCollection itemCollection) {
    WaitForAllFutures<Item> allAdded = new WaitForAllFutures<>();

    itemCollection.add(smallAngryPlanet, allAdded.notifySuccess(), v -> { });
    itemCollection.add(nod, allAdded.notifySuccess(), v -> { });
    itemCollection.add(uprooted, allAdded.notifySuccess(), v -> { });

    allAdded.waitForCompletion();
  }

  private Item smallAngryPlanet() {
    return new Item(null,
      null,
      null,
      new Status(ItemStatusName.AVAILABLE), bookMaterialTypeId, canCirculateLoanTypeId, null)
      .withBarcode("036000291452")
      .withTemporaryLocationId(annexLibraryLocationId)
      // Have to set call number components directly,
      // otherwise we will get timeout on retrieve holdings in preprocessor
      .withItemLevelCallNumber("123456")
      .withItemLevelCallNumberPrefix("prefix")
      .withItemLevelCallNumberSuffix("suffix")
      .withItemLevelCallNumberTypeId(UUID.randomUUID().toString());
  }

  private Item nod() {
    return new Item(null,
      null,
      null,
      new Status(ItemStatusName.AVAILABLE), bookMaterialTypeId, canCirculateLoanTypeId, null)
      .withBarcode("565578437802")
      .withTemporaryLocationId(annexLibraryLocationId)
      .withItemLevelCallNumber("123456")
      .withItemLevelCallNumberPrefix("prefix")
      .withItemLevelCallNumberSuffix("suffix")
      .withItemLevelCallNumberTypeId(UUID.randomUUID().toString());
  }

  private Item uprooted() {
    return new Item(null,
      null,
      null,
      new Status(ItemStatusName.AVAILABLE), bookMaterialTypeId, canCirculateLoanTypeId, null)
      .withBarcode("657670342075")
      .withTemporaryLocationId(annexLibraryLocationId)
      .withItemLevelCallNumber("123456")
      .withItemLevelCallNumberPrefix("prefix")
      .withItemLevelCallNumberSuffix("suffix")
      .withItemLevelCallNumberTypeId(UUID.randomUUID().toString());
  }

  private Item temeraire() {
    return new Item(null,
      null,
      null,
      new Status(ItemStatusName.AVAILABLE), bookMaterialTypeId,
      canCirculateLoanTypeId, null)
      .withBarcode("232142443432")
      .withTemporaryLocationId(annexLibraryLocationId)
      .withItemLevelCallNumber("123456")
      .withItemLevelCallNumberPrefix("prefix")
      .withItemLevelCallNumberSuffix("suffix")
      .withItemLevelCallNumberTypeId(UUID.randomUUID().toString());
  }

  private Item interestingTimes() {
    return new Item(null,
      null,
      null,
      new Status(ItemStatusName.AVAILABLE), bookMaterialTypeId, canCirculateLoanTypeId, null)
      .withBarcode("56454543534")
      .withTemporaryLocationId(annexLibraryLocationId)
      .withItemLevelCallNumber("123456")
      .withItemLevelCallNumberPrefix("prefix")
      .withItemLevelCallNumberSuffix("suffix")
      .withItemLevelCallNumberTypeId(UUID.randomUUID().toString());
  }

  private Item getItem(List<Item> allItems, String barcode) {
    return allItems.stream()
      .filter(it -> StringUtils.equals(it.getBarcode(), barcode))
      .findFirst().orElse(null);
  }
}
