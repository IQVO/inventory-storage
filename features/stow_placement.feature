# Derived from:
#   - docs/docs/adr/0009-product-classification-as-sku-master-data.md — a
#     Hazmat SKU may only be stowed into a hazmat-rated bin; a
#     TemperatureSensitive SKU only into a bin whose zone temperature class
#     matches; classified, rule-relevant SKUs fail CLOSED when the
#     facility-layout lookup is unavailable, every other SKU is unaffected.
#   - docs/docs/adr/0010-dot-hazard-class-and-same-bin-segregation.md —
#     incompatible DOT hazard classes may not share a bin.
#   - apis/openapi.yaml — POST /stowages 409 (hazmat-zone-required,
#     temperature-class-mismatch, location-classification-unavailable,
#     hazmat-class-incompatible).

Feature: Stow placement rules
  A SKU's classification, owned by product-master, constrains where it may be
  stowed. The bin's zone attributes come from a synchronous read of
  facility-layout. A refused stow changes nothing: no StockUnit is created and
  the bin keeps its occupancy.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: A Hazmat SKU is refused a bin outside a hazmat-rated zone
    Given a Bin "H-1-1" with capacity 10
    And Bin "H-1-1" sits in a non-hazmat "Ambient" zone
    And SKU "SKU-H" is classified by product-master as Hazmat with DOT hazard class 3
    When I Stow 2 units of SKU "SKU-H" into Bin "H-1-1"
    Then the response status is 409
    And the problem detail type is "hazmat-zone-required"
    And the Usable inventory for SKU "SKU-H" is 0
    When I look up Bin "H-1-1"
    Then the Bin response reports capacity 10, occupied 0 and available 10

  @bdd
  Scenario: A TemperatureSensitive SKU is refused a bin in a different temperature zone
    Given a Bin "T-1-1" with capacity 10
    And Bin "T-1-1" sits in a non-hazmat "Ambient" zone
    And SKU "SKU-ICE" is classified by product-master as TemperatureSensitive requiring "Frozen" storage
    When I Stow 2 units of SKU "SKU-ICE" into Bin "T-1-1"
    Then the response status is 409
    And the problem detail type is "temperature-class-mismatch"
    And the Usable inventory for SKU "SKU-ICE" is 0

  @bdd
  Scenario: An unavailable zone lookup blocks a classified SKU but never an unclassified one
    Given a Bin "L-1-1" with capacity 10
    And SKU "SKU-H" is classified by product-master as Hazmat with DOT hazard class 8
    And the facility-layout classification lookup is unavailable
    When I Stow 1 unit of SKU "SKU-H" into Bin "L-1-1"
    Then the response status is 409
    And the problem detail type is "location-classification-unavailable"
    When I Stow 1 unit of SKU "SKU-PLAIN" into Bin "L-1-1"
    Then the response status is 201

  @bdd
  Scenario: A SKU with an incompatible DOT hazard class is refused a bin that already holds another
    Given a Bin "D-1-1" with capacity 20
    And Bin "D-1-1" sits in a hazmat-rated zone
    And SKU "SKU-FLAM" is classified by product-master as Hazmat with DOT hazard class 3
    And SKU "SKU-OXI" is classified by product-master as Hazmat with DOT hazard class 5
    And 4 units of SKU "SKU-FLAM" are Stowed into Bin "D-1-1"
    When I Stow 2 units of SKU "SKU-OXI" into Bin "D-1-1"
    Then the response status is 409
    And the problem detail type is "hazmat-class-incompatible"
    And the Usable inventory for SKU "SKU-OXI" is 0

  @bdd
  Scenario: Compatible DOT hazard classes share a bin, and a SKU never conflicts with its own earlier stow
    Given a Bin "D-2-1" with capacity 20
    And a Bin "D-2-2" with capacity 20
    And Bin "D-2-1" sits in a hazmat-rated zone
    And Bin "D-2-2" sits in a hazmat-rated zone
    And SKU "SKU-FLAM" is classified by product-master as Hazmat with DOT hazard class 3
    And SKU "SKU-SOLID" is classified by product-master as Hazmat with DOT hazard class 4
    And SKU "SKU-GAS" is classified by product-master as Hazmat with DOT hazard class 2
    And 4 units of SKU "SKU-FLAM" are Stowed into Bin "D-2-1"
    When I Stow 2 units of SKU "SKU-SOLID" into Bin "D-2-1"
    Then the response status is 201
    Given 2 units of SKU "SKU-GAS" are Stowed into Bin "D-2-2"
    When I Stow 3 units of SKU "SKU-GAS" into Bin "D-2-2"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-GAS" is 5
