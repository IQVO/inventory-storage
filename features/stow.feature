Feature: Stow
  Chaotic (random) stow: any SKU may occupy any free Bin, and the system
  records the exact Bin. A Stow is only valid with BOTH an item-scan and a
  location-scan, and it must never push a Bin past its capacity.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: Stowing an item into a bin with capacity succeeds
    Given a Bin "A-1-1" with capacity 10
    And 5 units of SKU "SKU-1" have been Received
    When I Stow 5 units of SKU "SKU-1" into Bin "A-1-1"
    Then the response status is 201
    And the StockUnit response reports SKU "SKU-1" in Bin "A-1-1" with quantity 5
    And the response has a Location header pointing at the created StockUnit
    And the domain event "ItemStowed" was published
    And the domain event "LocationRecorded" was published
    And the Usable inventory for SKU "SKU-1" is 5

  @bdd
  Scenario: Stowing into a bin at full capacity is rejected
    Given a Bin "A-1-2" with capacity 5
    And 5 units of SKU "SKU-2" are Stowed into Bin "A-1-2"
    When I Stow 1 unit of SKU "SKU-2" into Bin "A-1-2"
    Then the response status is 409
    And the problem detail type is "bin-full"
    And the Usable inventory for SKU "SKU-2" is 5

  # Derived from apis/openapi.yaml — POST /stock/stow (stowStock): 404
  # bin-not-found, "The target bin does not exist." A location-scan
  # against a bin the system does not know is not a Stow: without a known
  # Bin there is no StockUnit, so nothing becomes usable.

  @bdd
  Scenario: Stowing into an unknown bin is rejected
    Given 5 units of SKU "SKU-3" have been Received
    When I Stow 5 units of SKU "SKU-3" into Bin "NOPE-1"
    Then the response status is 404
    And the problem detail type is "bin-not-found"
    And the Usable inventory for SKU "SKU-3" is 0
