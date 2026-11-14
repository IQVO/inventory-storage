# Documentation-derived rules for the Container aggregate canvas (C4, C7, C9),
# the "idempotent stow" corrective policy and the whole-case exit paths, plus
# the transfer-stow capacity edge of use case #16.
#
# Derived from:
#   - aggregate-design-canvas.md "Container": C7 "a container never changes
#     site: the first stow sets it from the bin, a later stow must stay in that
#     site" (container-not-at-site), C9 "a reserved case is never re-stowed or
#     broken", corrective policy "Idempotent stow. Stowing a container into the
#     bin it already occupies changes nothing; re-stowing within the same site
#     moves its SEALED_CASE unit"; handled command "Empty whole ... a confirmed
#     whole-case pick or a cycle-count shortfall".
#   - use-cases.md #16 StowTransferStock: "One StockUnit per bin leg is created
#     at the destination site, bin occupancy is taken ... all in ONE unit of
#     work", a refused stow leaves the receipt STAGED.

Feature: Container custody rules and transfer stow capacity
  A sealed case keeps one site for life and one bin at a time.

  Background:
    Given an empty warehouse
    And product-master has declared a CASE of 12 eaches for SKU "SKU-C" with GTIN "24006381333935"
    And a Bin "S1-A-Z-1-1-1-1" with capacity 1000 at site "S1" that accepts "CASE"
    And a Bin "S1-A-Z-1-1-1-4" with capacity 1000 at site "S1" that accepts "CASE"
    And a Bin "S2-A-Z-1-1-1-1" with capacity 1000 at site "S2" that accepts "CASE"
    And a sealed case "LPN-1" of SKU "SKU-C" for owner "seller-a" is stowed in Bin "S1-A-Z-1-1-1-1"

  @bdd
  Scenario: Stowing a container into the bin it already occupies changes nothing
    When I Stow Container "LPN-1" into Bin "S1-A-Z-1-1-1-1"
    Then the response status is 200
    And Container "LPN-1" is stowed in Bin "S1-A-Z-1-1-1-1" at site "S1"
    And Bin "S1-A-Z-1-1-1-1" reports 12 occupied
    And the domain event "ContainerStowed" was published exactly 1 time

  @bdd
  Scenario: Re-stowing a container inside its site moves its stock to the new bin
    When I Stow Container "LPN-1" into Bin "S1-A-Z-1-1-1-4"
    Then the response status is 200
    And Container "LPN-1" is stowed in Bin "S1-A-Z-1-1-1-4" at site "S1"
    And Bin "S1-A-Z-1-1-1-1" reports 0 occupied
    And Bin "S1-A-Z-1-1-1-4" reports 12 occupied
    And the Usable inventory for SKU "SKU-C" and owner "seller-a" is 12

  @bdd
  Scenario: A container never changes site
    When I Stow Container "LPN-1" into Bin "S2-A-Z-1-1-1-1"
    Then the response status is 409
    And the problem detail type is "container-not-at-site"
    And Container "LPN-1" is stowed in Bin "S1-A-Z-1-1-1-1" at site "S1"
    And Bin "S2-A-Z-1-1-1-1" reports 0 occupied

  @bdd
  Scenario: A reserved case is never re-stowed
    Given I Reserve 1 full case of SKU "SKU-C" for demand "D-1" for owner "seller-a"
    When I Stow Container "LPN-1" into Bin "S1-A-Z-1-1-1-4"
    Then the response status is 409
    And the problem detail type is "container-reserved"
    And Container "LPN-1" is stowed in Bin "S1-A-Z-1-1-1-1" at site "S1"
    And Bin "S1-A-Z-1-1-1-4" reports 0 occupied

  @bdd
  Scenario: A cycle-count shortfall empties the container whose stock went missing
    When I run a Cycle count on Bin "S1-A-Z-1-1-1-1" with counted quantity 0
    Then the response status is 200
    And Container "LPN-1" is EMPTY
    And the domain event "ContainerEmptied" was published exactly 1 time
    And the Usable inventory for SKU "SKU-C" and owner "seller-a" is 0

  @bdd
  Scenario: An unknown container is container-not-found
    When I look up Container "LPN-NOPE"
    Then the response status is 404
    And the problem detail type is "container-not-found"
