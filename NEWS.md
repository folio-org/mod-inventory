## 11.3.0 2019-03-15

* Optimizes use of HttpClients (MODINV-102)
* Use Alpine docker container (MODINV-100)
* Return new `Item` as JSON on POST (MODINV-106)

## 11.2.0 2019-02-19

* Provides `inventory` interface 8.3 (MODINV-104)
* Requires `item-storage` interface 7.3 (MODINV-104)
* Adds `status.date` to `item` schema (MODINV-104)

## 11.1.0 2019-02-01

* Provides `inventory` interface 8.2 (MODINV-101 MODINV-103)
* Requires `item-storage` interface 7.2 (MODINV-101 MODINV-103)
* Adds `circulationNotes' to `item` schema (MODINV-101)
* Adds `purchaseOrderLineIdentifier` to `item` schema (MODINV-103)

## 11.0.0 2018-11-30

* Provides `inventory` interface 8.0 (MODINV-90)
* Requires `holdings-storage` interface 2.0 or 3.0 (MODINV-93)
* Requires `instance-storage` interface 6.0 (MODINV-90)
* Requires `item-storage` interface 7.0 (MODINV-94)
* Changes structure of `alternativeTitles` (MODINV-90)
* Renames `item.pieceIdentifiers` to `copyNumbers` (MODINV-94)
* Changes structure of `item` `notes` (MODINV-97)
* Renames, changes structure of `instance.statisticalCodes` (MODINV-98) 
* Adds 20 new properties to `item` (MODINV-72)

## 10.0.0 2018-10-12

* Propagates `instance.indexTitle` (MODINV-88)
* Requires `locations` interface 2.0 or 3.0 (MODINV-89)
* Upgrades to RAML 1.0 (MODINV-85)
* Removes `instance.edition` (MODINV-78)
* Adds `instance.editions` (MODINV-78)
* Removes `instanceFormatId` (MODINV-86)
* Adds `instanceFormatIds` (MODINV-86)
* Provides `inventory` interface 7.0 (MODINV-83)
* Requires `instance-storage` interface version 5.0 (MODINV-83)
* Removes property `instance.urls` (MODINV-81)
* Removes property `categoryLevelId` (MODINV-82)
* Renames `instance.electronicAccess.relationship` to `relationshipId` (MODINV-84)

## 9.5.0 2018-09-13

* Propagates more properties from `instance-storage` (MODINV-71)
* Provides `inventory` interface 6.4 (MODINV-71)
* Requires `instance-storage` interface version 4.6 (MODINV-71)

## 9.4.0 2018-09-10

* Implements Instance relationships (MODINV-69)
* Adds optional Instance properties `parentInstances`, `childInstances` (MODINV-69)
* Provides `inventory` interface 6.3 (MODINV-69)
* Requires `instance-storage` interface version 4.5 (MODINV-69)

## 9.3.0 2018-08-30

* Include `callNumber` from holding in item (if present, MODINV-70)
* Provides `inventory` 6.2 interface (MODINV-70)

## 9.2.0 2018-08-27

* Add properties `alternativeTitles`, `edition`, `series`, `subjects`, `classifications`,
  `publication`, `urls`, `instanceFormatId`, `physicalDescriptions`, `languages`,
  `notes`, `sourceRecordFormat`, `metadata` to `Instance` schema (MODINV-68)

## 9.1.0 2018-07-10

* Reference record lookups use == CQL relation, instead of = (MODINV-64)

## 9.0.0 2018-07-06

* Includes item `effectiveLocation` property for overall location of item (MODINV-56)
* Changes item `permanentLocation` to be property from storage instead of derived (MODINV-56)
* Provides `inventory` interface 6.0 (MODINV-56)
* Requires `item-storage` interface 5.3 (MODINV-56)
* Requires `holdings-storage` interface 1.3 (MODINV-56)
* Requires `instance-types` interface 1.0 or 2.0 (MODINV-60)

## 8.0.0 2018-05-15

* Expose change `metadata` property for items (MODINV-47)
* Extend the offset and limit paging query parameters to allow maximum integer values (MODINV-55)
* Provides `inventory` interface 5.2 (MODINV-47, MODINV-55)
* Requires `locations` interface 1.0 or 2.0 (MODINV-49)
* No longer requires `shelf-locations` interface (MODINV-49)

## 7.1.0 2018-04-03

* Use exact match for barcode and ID lookups using CQL (MODINV-43)
* Add optional field `contributorTypeText` to `instance.contributors` (MODINV-52)
* Provides inventory interface version 5.1
* Requires instance-storage interface version 4.2

## 7.0.0 2018-01-09

* Require `holdingsRecordId` from `item` (MODINV-39)
* Removes `title` from `item` (MODINV-37)
* Removes `instanceId` from `item` (MODINV-38)
* Removes `permanentLocationId` from `item` (MODINV-37)
* Removes `creators` from `instance` (MODINV-41)
* Adds `contributorNameTypeId` to `contributors` in `instances` (MODINV-41)
* Provides inventory interface version 5.0
* Requires item-storage interface version 5.0
* Requires instance-storage interface version 4.0

## 6.0.0 2017-12-20

* Removes `location` property from `item` record
* `title` is now optional for an `item` (MODINV-34)
* Adds `holdingsRecordId` to item (MODINV-30)
* MODS ingest now creates holdings records (MODINV-30)
* Introduces `enumeration`, `chronology`, `numberOfPieces`, `notes`, `pieceIdentifiers` (MODINV-33)
* Introduces `permanentLocation` and `temporaryLocation` properties (which include the name fetched from `shelf-locations`)
* Provides inventory interface version 4.2
* Requires item-storage interface version 4.1
* Requires instance-storage interface version 3.0
* Requires holding-storage interface version 1.0
* Requires shelf-locations interface version 1.0
* Requires instance-types interface version 1.0
* Requires identifier-types interface version 1.0
* Requires contributor-name-types interface version 1.0
* Adds mod- prefix to names of the built artifacts (FOLIO-813)

## 5.1.1 2017-09-01

* Generates Descriptors at build time from templates in ./descriptors (FOLIO-701)
* Remove `module.items.enabled` and `inventory-storage.all` permissions from
`inventory.all` set, as part of moving permissions to UI modules (MODINV-23)

## 5.1.0 2017-08-03

* MODINVSTOR-12 Searching and sorting on material type properties (e.g. materialType.name)
* Provides inventory interface version 2.1 (notes additional CQL indexes in comments)
* Requires item-storage interface version 3.1
* Include implementation version in `id` in Module Descriptor
* MODINV-18 - Fix for unique barcode check when changing barcode

## 5.0.0 2017-06-07

* Items do not require relating to an instance (instanceId is optional)
* Items do not require a barcode (no uniqueness check performed when not supplied)
* Items require a title
* Items require a reference to a material type
* Items require a reference to a permanent loan type
* Inventory.all permission set includes permissions for related UI tasks
* Requires item-storage interface version 3.0
* Requires instance-storage interface version 2.0
* Requires material-types interface version 2.0
* Requires loan-types interface version 2.0

## 4.4.0 2017-05-31

* Makes the all inventory permissions set visible (requires Okapi 1.3.0)
* Includes permission definition for enabling the items UI module (included in all permissions set)

## 4.3.0 2017-05-09

* Include total record count in item and instance collection resources

## 4.2.0 2017-05-08

* Provide permanent and temporary loan type associations for items
* Include name of loan type in item representation (by querying loan type controlled vocabulary)

## 4.0.0 2017-04-25

* Use UUID to reference material types

## 3.0.0 2017-04-04

* Required permissions for requests

## 2.0.0 2017-04-03

* Initial release
