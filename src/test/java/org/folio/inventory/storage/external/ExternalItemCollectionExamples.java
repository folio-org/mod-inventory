package org.folio.inventory.storage.external

import org.folio.inventory.common.WaitForAllFutures
import org.folio.inventory.common.api.request.PagingParameters
import org.folio.inventory.common.domain.MultipleRecords
import org.folio.inventory.common.domain.Success
import org.folio.inventory.domain.CollectionProvider
import org.folio.inventory.domain.Item
import org.folio.inventory.domain.ItemCollection
import org.junit.Before
import org.junit.Test

import java.util.concurrent.CompletableFuture

import static org.folio.inventory.common.FutureAssistance.*

class ExternalItemCollectionExamples {

  private final String bookMaterialTypeId = UUID.randomUUID().toString()
  private final String canCirculateLoanTypeId = UUID.randomUUID().toString()

  private final String mainLibraryLocationId = UUID.randomUUID().toString()
  private final String annexLibraryLocationId = UUID.randomUUID().toString()

  private static final String firstTenantId = "test_tenant_1"
  private static final String secondTenantId = "test_tenant_2"
  private static final String firstTenantToken = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiIsInRlbmFudCI6ImRlbW9fdGVuYW50In0.29VPjLI6fLJzxQW0UhQ0jsvAn8xHz501zyXAxRflXfJ9wuDzT8TDf-V75PjzD7fe2kHjSV2dzRXbstt3BTtXIQ"
  private static final String secondTenantToken = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiIsInRlbmFudCI6ImRlbW9fdGVuYW50In0.63jTgc15Kil946OdOGYZur_8xVWEUURANx87FAOQajh9TJbsnCMbjE164JQqNLMWShCyi9FOX0Kr1RFuiHTFAQ"

  private final CollectionProvider collectionProvider = ExternalStorageSuite.useVertx({
    new ExternalStorageCollections(it, ExternalStorageSuite.instanceStorageAddress) });

  private final Item smallAngryPlanet = smallAngryPlanet()
  private final Item nod = nod()
  private final Item uprooted = uprooted()
  private final Item temeraire = temeraire()
  private final Item interestingTimes = interestingTimes()

  @Before
  public void before() {
    emptyCollection(collectionProvider.getItemCollection(firstTenantId, firstTenantToken))
    emptyCollection(collectionProvider.getItemCollection(secondTenantId, secondTenantToken))
  }

  @Test
  void canBeEmptied() {
    def collection = collectionProvider.getItemCollection(firstTenantId, firstTenantToken)

    addSomeExamples(collection)

    def emptied = new CompletableFuture<Void>()

    collection.empty(succeed(emptied), fail(emptied))

    waitForCompletion(emptied)

    def findFuture = new CompletableFuture<MultipleRecords<Item>>()

    collection.findAll(PagingParameters.defaults(),
      succeed(findFuture), fail(findFuture))

    def wrappedItems = getOnCompletion(findFuture)

    def allItems = wrappedItems.records

    assert allItems.size() == 0
    assert wrappedItems.totalRecords == 0
  }

  @Test
  void anItemCanBeAdded() {
    def collection = collectionProvider.getItemCollection(firstTenantId, firstTenantToken)

    addSomeExamples(collection)

    def findFuture = new CompletableFuture<MultipleRecords<Item>>()

    collection.findAll(PagingParameters.defaults(), succeed(findFuture),
      fail(findFuture))

    def wrappedItems = getOnCompletion(findFuture)

    def allItems = wrappedItems.records

    assert allItems.size() == 3
    assert wrappedItems.totalRecords == 3

    assert allItems.every { it.id != null }
    assert allItems.every { it.title != null }
    assert allItems.every { it.barcode != null }
    assert allItems.every { it.instanceId != null }

    def smallAngry = allItems.find({ it.title == "Long Way to a Small Angry Planet" })

    assert smallAngry != null
    assert smallAngry.barcode == "036000291452"
    assert smallAngry.status == "Available"
    assert smallAngry.materialTypeId == "${bookMaterialTypeId}"
    assert smallAngry.permanentLoanTypeId == "${canCirculateLoanTypeId}"
    assert smallAngry.permanentLocationId == mainLibraryLocationId
    assert smallAngry.temporaryLocationId == annexLibraryLocationId

    def nod = allItems.find({ it.title == "Nod" })

    assert nod != null
    assert nod.barcode == "565578437802"
    assert nod.status == "Available"
    assert nod.materialTypeId == "${bookMaterialTypeId}"
    assert nod.permanentLoanTypeId == "${canCirculateLoanTypeId}"
    assert nod.permanentLocationId == mainLibraryLocationId
    assert nod.temporaryLocationId == annexLibraryLocationId

    def uprooted = allItems.find({ it.title == "Uprooted"})

    assert uprooted != null
    assert uprooted.barcode == "657670342075"
    assert uprooted.status == "Available"
    assert uprooted.materialTypeId == "${bookMaterialTypeId}"
    assert uprooted.permanentLoanTypeId == "${canCirculateLoanTypeId}"
    assert uprooted.permanentLocationId == mainLibraryLocationId
    assert uprooted.temporaryLocationId == annexLibraryLocationId

  }

  @Test
  void anItemCanBeAddedWithAnId() {
    def collection = collectionProvider.getItemCollection(firstTenantId, firstTenantToken)

    def addFinished = new CompletableFuture<Item>()

    def itemId = UUID.randomUUID().toString()

    def itemWithId = smallAngryPlanet.copyWithNewId(itemId)

    collection.add(itemWithId, succeed(addFinished), fail(addFinished))

    def added = getOnCompletion(addFinished)

    assert added.id == itemId
  }

  @Test
  void anItemCanBeUpdated() {
    def collection = collectionProvider.getItemCollection(firstTenantId, firstTenantToken)

    def addFinished = new CompletableFuture<Item>()

    collection.add(smallAngryPlanet, succeed(addFinished), fail(addFinished))

    def added = getOnCompletion(addFinished)

    def updateFinished = new CompletableFuture<Item>()

    def changed = added.changeStatus("Checked Out")

    collection.update(changed, succeed(updateFinished),
      fail(updateFinished))

    waitForCompletion(updateFinished)

    def gotUpdated = new CompletableFuture<Item>()

    collection.findById(added.id, succeed(gotUpdated),
      fail(gotUpdated))

    def updated = getOnCompletion(gotUpdated)

    assert updated.id == added.id
    assert updated.title == added.title
    assert updated.barcode == added.barcode
    assert updated.permanentLocationId == added.permanentLocationId
    assert updated.temporaryLocationId == added.temporaryLocationId
    assert updated.materialTypeId == added.materialTypeId
    assert updated.permanentLoanTypeId == added.permanentLoanTypeId
    assert updated.status == "Checked Out"
  }

  @Test
  void anItemCanBeFoundByIdWithinATenant() {
    def firstTenantCollection = collectionProvider
      .getItemCollection(firstTenantId, firstTenantToken)

    def secondTenantCollection = collectionProvider
      .getItemCollection(secondTenantId, secondTenantToken)

    def addFuture = new CompletableFuture<Item>()

    firstTenantCollection.add(smallAngryPlanet, succeed(addFuture),
      fail(addFuture))

    def addedItem = getOnCompletion(addFuture)

    def findItemForCorrectTenant = new CompletableFuture<Item>()
    def findItemForIncorrectTenant = new CompletableFuture<Item>()

    firstTenantCollection.findById(addedItem.id,
      succeed(findItemForCorrectTenant), fail(findItemForCorrectTenant))

    secondTenantCollection.findById(addedItem.id,
      succeed(findItemForIncorrectTenant), fail(findItemForIncorrectTenant))

    assert getOnCompletion(findItemForCorrectTenant) != null
    assert getOnCompletion(findItemForIncorrectTenant) == null
  }

  @Test
  void anItemCanBeDeleted() {
    def collection = collectionProvider.getItemCollection(firstTenantId, firstTenantToken)

    addSomeExamples(collection)

    def itemToBeDeletedFuture = new CompletableFuture<Item>()

    collection.add(temeraire(), succeed(itemToBeDeletedFuture),
      fail(itemToBeDeletedFuture))

    def itemToBeDeleted = itemToBeDeletedFuture.get()

    def deleted = new CompletableFuture<Void>()

    collection.delete(itemToBeDeleted.id, succeed(deleted), fail(deleted))

    waitForCompletion(deleted)

    def findFuture = new CompletableFuture<Success<Item>>()

    collection.findById(itemToBeDeleted.id, succeed(findFuture),
      fail(findFuture))

    assert findFuture.get() == null

    def findAllFuture = new CompletableFuture<MultipleRecords<Item>>()

    collection.findAll(PagingParameters.defaults(), succeed(findAllFuture),
      fail(findAllFuture))

    def wrappedItems = getOnCompletion(findAllFuture)

    def allItems = wrappedItems.records

    assert allItems.size() == 3
    assert wrappedItems.totalRecords == 3
  }

  @Test
  void allItemsCanBePaged() {
    def collection = collectionProvider.getItemCollection(firstTenantId, firstTenantToken)

    def allAdded = new WaitForAllFutures()

    collection.add(smallAngryPlanet, allAdded.notifySuccess(), {})
    collection.add(nod, allAdded.notifySuccess(), {})
    collection.add(uprooted, allAdded.notifySuccess(), {})
    collection.add(temeraire, allAdded.notifySuccess(), {})
    collection.add(interestingTimes, allAdded.notifySuccess(), {})

    allAdded.waitForCompletion()

    def firstPageFuture = new CompletableFuture<MultipleRecords<Item>>()
    def secondPageFuture = new CompletableFuture<MultipleRecords<Item>>()

    collection.findAll(new PagingParameters(3, 0), succeed(firstPageFuture),
      fail(secondPageFuture))

    collection.findAll(new PagingParameters(3, 3), succeed(secondPageFuture),
      fail(secondPageFuture))

    def firstPage = getOnCompletion(firstPageFuture)
    def secondPage = getOnCompletion(secondPageFuture)

    assert firstPage.records.size() == 3
    assert secondPage.records.size() == 2

    assert firstPage.totalRecords == 5
    assert secondPage.totalRecords == 5
  }

  @Test
  void itemsCanBeFoundByByPartialName() {
    def collection = collectionProvider.getItemCollection(firstTenantId, firstTenantToken)

    def firstAddFuture = new CompletableFuture<Item>()
    def secondAddFuture = new CompletableFuture<Item>()
    def thirdAddFuture = new CompletableFuture<Item>()

    collection.add(smallAngryPlanet, succeed(firstAddFuture),
      fail(firstAddFuture))
    collection.add(nod, succeed(secondAddFuture),
      fail(secondAddFuture))
    collection.add(uprooted, succeed(thirdAddFuture),
      fail(thirdAddFuture))

    def allAddsFuture = CompletableFuture.allOf(firstAddFuture,
      secondAddFuture, thirdAddFuture)

    getOnCompletion(allAddsFuture)

    def addedSmallAngryPlanet = getOnCompletion(firstAddFuture)

    def findFuture = new CompletableFuture<MultipleRecords<Item>>()

    collection.findByCql("title=\"*Small Angry*\"", new PagingParameters(10, 0),
      succeed(findFuture), fail(findFuture))

    def wrappedItems = getOnCompletion(findFuture)

    assert wrappedItems.records.size() == 1
    assert wrappedItems.totalRecords == 1

    assert wrappedItems.records[0].id == addedSmallAngryPlanet.id
  }

  @Test
  void itemsCanBeFoundByBarcode() {
    def collection = collectionProvider.getItemCollection(firstTenantId, firstTenantToken)

    def firstAddFuture = new CompletableFuture<Item>()
    def secondAddFuture = new CompletableFuture<Item>()
    def thirdAddFuture = new CompletableFuture<Item>()

    collection.add(smallAngryPlanet, succeed(firstAddFuture),
      fail(firstAddFuture))
    collection.add(nod, succeed(secondAddFuture),
      fail(secondAddFuture))
    collection.add(uprooted, succeed(thirdAddFuture),
      fail(thirdAddFuture))

    def allAddsFuture = CompletableFuture.allOf(secondAddFuture, thirdAddFuture)

    getOnCompletion(allAddsFuture)

    def addedSmallAngryPlanet = getOnCompletion(firstAddFuture)

    def findFuture = new CompletableFuture<MultipleRecords<Item>>()

    collection.findByCql("barcode=036000291452", new PagingParameters(10, 0),
      succeed(findFuture), fail(findFuture))

    def wrappedItems = getOnCompletion(findFuture)

    assert wrappedItems.records.size() == 1
    assert wrappedItems.totalRecords == 1

    assert wrappedItems.records[0].id == addedSmallAngryPlanet.id
  }

  @Test
  void anItemCanBeFoundById() {
    def collection = collectionProvider.getItemCollection(firstTenantId, firstTenantToken)

    def firstAddFuture = new CompletableFuture<Item>()
    def secondAddFuture = new CompletableFuture<Item>()

    collection.add(smallAngryPlanet, succeed(firstAddFuture),
      fail(firstAddFuture))

    collection.add(nod, succeed(secondAddFuture),
      fail(secondAddFuture))

    def addedItem = getOnCompletion(firstAddFuture)
    def otherAddedItem = getOnCompletion(secondAddFuture)

    def findFuture = new CompletableFuture<Item>()
    def otherFindFuture = new CompletableFuture<Item>()

    collection.findById(addedItem.id, succeed(findFuture),
      fail(findFuture))

    collection.findById(otherAddedItem.id, succeed(otherFindFuture),
      fail(otherFindFuture))

    def foundItem = getOnCompletion(findFuture)
    def otherFoundItem = getOnCompletion(otherFindFuture)

    assert foundItem.title == "Long Way to a Small Angry Planet"
    assert foundItem.instanceId == smallAngryPlanet.instanceId
    assert foundItem.barcode == "036000291452"
    assert foundItem.status == "Available"
    assert foundItem.materialTypeId == "${bookMaterialTypeId}"
    assert foundItem.permanentLoanTypeId == "${canCirculateLoanTypeId}"
    assert foundItem.permanentLocationId == mainLibraryLocationId
    assert foundItem.temporaryLocationId == annexLibraryLocationId

    assert otherFoundItem.title == "Nod"
    assert otherFoundItem.instanceId == nod.instanceId
    assert otherFoundItem.barcode == "565578437802"
    assert otherFoundItem.status == "Available"
    assert otherFoundItem.materialTypeId == "${bookMaterialTypeId}"
    assert otherFoundItem.permanentLoanTypeId == "${canCirculateLoanTypeId}"
    assert otherFoundItem.permanentLocationId == mainLibraryLocationId
    assert otherFoundItem.temporaryLocationId == annexLibraryLocationId
  }

  private void addSomeExamples(ItemCollection itemCollection) {
    def allAdded = new WaitForAllFutures()

    itemCollection.add(smallAngryPlanet, allAdded.notifySuccess(), { })
    itemCollection.add(nod, allAdded.notifySuccess(), { })
    itemCollection.add(uprooted, allAdded.notifySuccess(), { })

    allAdded.waitForCompletion()
  }

  private void emptyCollection(ItemCollection collection) {
    def emptied = new CompletableFuture()

    collection.empty(succeed(emptied), fail(emptied))

    waitForCompletion(emptied)
  }

  private Item smallAngryPlanet() {
    new Item(null, "Long Way to a Small Angry Planet", "036000291452",
      UUID.randomUUID().toString(), "Available", bookMaterialTypeId,
      mainLibraryLocationId, annexLibraryLocationId, canCirculateLoanTypeId, null)
  }

  private Item nod() {
    new Item(null, "Nod", "565578437802",
      UUID.randomUUID().toString(), "Available", bookMaterialTypeId,
      mainLibraryLocationId, annexLibraryLocationId, canCirculateLoanTypeId, null)
  }

  private Item uprooted() {
    new Item(null, "Uprooted", "657670342075",
      UUID.randomUUID().toString(), "Available", bookMaterialTypeId,
      mainLibraryLocationId, annexLibraryLocationId, canCirculateLoanTypeId, null)
  }

  private Item temeraire() {
    new Item(null, "Temeraire", "232142443432",
      UUID.randomUUID().toString(), "Available", bookMaterialTypeId,
      mainLibraryLocationId, annexLibraryLocationId, canCirculateLoanTypeId, null)
  }

  private Item interestingTimes() {
    new Item(null, "Interesting Times", "56454543534",
      UUID.randomUUID().toString(), "Available", bookMaterialTypeId,
      mainLibraryLocationId, annexLibraryLocationId, canCirculateLoanTypeId, null)
  }
}
