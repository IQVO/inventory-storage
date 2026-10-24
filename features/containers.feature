# Derived from:
#   - docs/docs/adr/0045-container-lpn-sealed-cases-breakcase-owner-segregated-stock.md and
#     docs/docs/adr/0046-container-slice-amends-0045.md — a Container (LPN) gives a
#     sealed case an identity in storage; the ledger stays in eaches; BreakCase conserves
#     quantity; stock and reservations are segregated by ownerRef.
#   - warehouse-systems .hermes/plans/mastercase-briefs/contracts.md sections 2, 3, 7, 8 —
#     the double-count rule: ReceiptLineReceived is the sole quantity authority,
#     ReceiptCartonReceived only creates/links Containers, in either arrival order.
#   - The two events arrive from Kafka, so they are seeded through the SAME use cases the
#     consumers call (BookInboundReceiptLine, LinkReceiptCarton, ApplyPackLevel); every
#     assertion goes through REST or the published domain events.

Feature: Containers (LPN), sealed cases and owner-segregated stock
  A sealed case keeps its identity from the dock to the door. Eaches stay the ledger unit.

  Background:
    Given an empty warehouse
    And product-master has declared a CASE of 12 eaches for SKU "SKU-C" with GTIN "24006381333935"
    And a Bin "S1-A-Z-1-1-1-1" with capacity 1000 at site "S1" that accepts "CASE"
    And a Bin "S1-A-Z-1-1-1-2" with capacity 1000 at site "S1" that accepts "EACH"

  @bdd
  Scenario: Three sealed cases become three containers and 36 eaches are booked once (cartons first)
    When inbound-receiving reports sealed carton "LPN-1" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    And inbound-receiving reports sealed carton "LPN-2" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    And inbound-receiving reports sealed carton "LPN-3" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    Then the domain event "ContainerRegistered" was published exactly 0 times
    And the domain event "StockReceived" was published exactly 0 times
    When inbound-receiving reports line 1 of receipt "R-1" of 36 units of SKU "SKU-C" for owner "seller-a" in Good condition
    Then the domain event "ContainerRegistered" was published exactly 3 times
    And the domain event "StockReceived" was published exactly 1 time
    And the staged quantity published for SKU "SKU-C" is 36 eaches
    And Container "LPN-2" is SEALED and holds 12 eaches of SKU "SKU-C" for owner "seller-a"

  @bdd
  Scenario: Three sealed cases become three containers and 36 eaches are booked once (line first)
    When inbound-receiving reports line 1 of receipt "R-1" of 36 units of SKU "SKU-C" for owner "seller-a" in Good condition
    And inbound-receiving reports sealed carton "LPN-1" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    And inbound-receiving reports sealed carton "LPN-2" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    And inbound-receiving reports sealed carton "LPN-3" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    Then the domain event "ContainerRegistered" was published exactly 3 times
    And the domain event "StockReceived" was published exactly 1 time
    And the staged quantity published for SKU "SKU-C" is 36 eaches
    And the Usable inventory for SKU "SKU-C" and owner "seller-a" is 0

  @bdd
  Scenario: Cartons beyond the booked quantity wait and never exist for uncounted units
    When inbound-receiving reports line 1 of receipt "R-1" of 24 units of SKU "SKU-C" for owner "seller-a" in Good condition
    And inbound-receiving reports sealed carton "LPN-1" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    And inbound-receiving reports sealed carton "LPN-2" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    And inbound-receiving reports sealed carton "LPN-3" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    Then the domain event "ContainerRegistered" was published exactly 2 times
    When I look up Container "LPN-3"
    Then the response status is 404

  @bdd
  Scenario: Mixed, opened and damaged cartons get no container
    When inbound-receiving reports line 1 of receipt "R-1" of 36 units of SKU "SKU-C" for owner "seller-a" in Good condition
    And inbound-receiving reports an opened carton "LPN-O" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1"
    And inbound-receiving reports a damaged carton "LPN-D" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1"
    And inbound-receiving reports a mixed carton "LPN-M" of SKU "SKU-C" and SKU "SKU-X" on line 1 of receipt "R-1"
    Then the domain event "ContainerRegistered" was published exactly 0 times

  @bdd
  Scenario: Duplicate delivery of either event is idempotent
    When inbound-receiving reports line 1 of receipt "R-1" of 12 units of SKU "SKU-C" for owner "seller-a" in Good condition
    And inbound-receiving reports line 1 of receipt "R-1" of 12 units of SKU "SKU-C" for owner "seller-a" in Good condition
    And inbound-receiving reports sealed carton "LPN-1" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    And inbound-receiving reports sealed carton "LPN-1" of 12 units of SKU "SKU-C" on line 1 of receipt "R-1" for owner "seller-a"
    And inbound-receiving reports sealed carton "LPN-1" again under a new event id on line 1 of receipt "R-1"
    Then the domain event "StockReceived" was published exactly 1 time
    And the domain event "ContainerRegistered" was published exactly 1 time

  @bdd
  Scenario: A container is stowed into a case bin and its site comes from the bin
    Given a sealed case "LPN-1" of SKU "SKU-C" for owner "seller-a" is received
    When I Stow Container "LPN-1" into Bin "S1-A-Z-1-1-1-1"
    Then the response status is 200
    And Container "LPN-1" is stowed in Bin "S1-A-Z-1-1-1-1" at site "S1"
    And the domain event "ContainerStowed" was published
    When I look up Bin "S1-A-Z-1-1-1-1"
    Then the Bin response reports capacity 1000, occupied 12 and available 988
    And the Usable inventory for SKU "SKU-C" and owner "seller-a" is 12

  @bdd
  Scenario: A container cannot be stowed into an each-only bin or a bin with no handling unit
    Given a Bin "S1-A-Z-1-1-1-3" with capacity 1000 at site "S1" that records no handling unit
    And a sealed case "LPN-1" of SKU "SKU-C" for owner "seller-a" is received
    When I Stow Container "LPN-1" into Bin "S1-A-Z-1-1-1-2"
    Then the response status is 409
    And the problem detail type is "bin-handling-unit-mismatch"
    When I Stow Container "LPN-1" into Bin "S1-A-Z-1-1-1-3"
    Then the response status is 409
    And the problem detail type is "bin-handling-unit-mismatch"
    And Container "LPN-1" is not stowed

  @bdd
  Scenario: Breaking a case moves exactly its units per pack to loose stock and empties the container
    Given a sealed case "LPN-1" of SKU "SKU-C" for owner "seller-a" is stowed in Bin "S1-A-Z-1-1-1-1"
    When I Break Container "LPN-1" into loose Bin "S1-A-Z-1-1-1-2"
    Then the response status is 200
    And Container "LPN-1" is EMPTY
    And the domain event "CaseBroken" was published
    And the domain event "ContainerEmptied" was published
    When I look up Bin "S1-A-Z-1-1-1-2"
    Then the Bin response reports capacity 1000, occupied 12 and available 988
    And the Usable inventory for SKU "SKU-C" and owner "seller-a" is 12
    And the loose usable inventory for SKU "SKU-C" and owner "seller-a" is 12

  @bdd
  Scenario: A broken (EMPTY) or unstowed container cannot be broken
    Given a sealed case "LPN-1" of SKU "SKU-C" for owner "seller-a" is stowed in Bin "S1-A-Z-1-1-1-1"
    And a sealed case "LPN-2" of SKU "SKU-C" for owner "seller-a" is received
    And I Break Container "LPN-1" into loose Bin "S1-A-Z-1-1-1-2"
    When I Break Container "LPN-1" into loose Bin "S1-A-Z-1-1-1-2"
    Then the response status is 409
    And the problem detail type is "container-not-sealed"
    When I Break Container "LPN-2" into loose Bin "S1-A-Z-1-1-1-2"
    Then the response status is 409
    And the problem detail type is "container-not-stowed"

  @bdd
  Scenario: A case cannot be broken into a case-only bin
    Given a sealed case "LPN-1" of SKU "SKU-C" for owner "seller-a" is stowed in Bin "S1-A-Z-1-1-1-1"
    When I Break Container "LPN-1" into loose Bin "S1-A-Z-1-1-1-1"
    Then the response status is 409
    And the problem detail type is "bin-handling-unit-mismatch"
    And Container "LPN-1" is SEALED and holds 12 eaches of SKU "SKU-C" for owner "seller-a"

  @bdd
  Scenario: Whole cases are reserved and a reserved case cannot be broken
    Given a sealed case "LPN-1" of SKU "SKU-C" for owner "seller-a" is stowed in Bin "S1-A-Z-1-1-1-1"
    And a sealed case "LPN-2" of SKU "SKU-C" for owner "seller-a" is stowed in Bin "S1-A-Z-1-1-1-1"
    When I Reserve 1 full case of SKU "SKU-C" for demand "D-1" for owner "seller-a"
    Then the response status is 201
    And the Reservation response reports quantity 12 for demand "D-1"
    And every Reservation allocation is picked from Bin "S1-A-Z-1-1-1-1"
    When I Reserve 2 full cases of SKU "SKU-C" for demand "D-2" for owner "seller-a"
    Then the response status is 409
    And the problem detail type is "insufficient-usable"
    When I Confirm the pick for the Reservation with id "res-1"
    Then the domain event "ContainerEmptied" was published exactly 1 time

  @bdd
  Scenario: A reserved case cannot be broken
    Given a sealed case "LPN-1" of SKU "SKU-C" for owner "seller-a" is stowed in Bin "S1-A-Z-1-1-1-1"
    And I Reserve 1 full case of SKU "SKU-C" for demand "D-1" for owner "seller-a"
    When I Break Container "LPN-1" into loose Bin "S1-A-Z-1-1-1-2"
    Then the response status is 409
    And the problem detail type is "container-reserved"

  @bdd
  Scenario: Owner A stock cannot satisfy an owner B reservation
    Given a sealed case "LPN-1" of SKU "SKU-C" for owner "seller-a" is stowed in Bin "S1-A-Z-1-1-1-1"
    And a sealed case "LPN-2" of SKU "SKU-C" for owner "seller-a" is stowed in Bin "S1-A-Z-1-1-1-1"
    And I Break Container "LPN-2" into loose Bin "S1-A-Z-1-1-1-2"
    When I Reserve 1 full case of SKU "SKU-C" for demand "D-b" for owner "seller-b"
    Then the response status is 409
    And the problem detail type is "insufficient-usable"
    When I Reserve 1 units of SKU "SKU-C" for demand "D-b2" for owner "seller-b"
    Then the response status is 409
    And the problem detail type is "insufficient-usable"
    When I Reserve 1 full case of SKU "SKU-C" for demand "D-f"
    Then the response status is 409
    When I Reserve 5 units of SKU "SKU-C" for demand "D-a" for owner "seller-a"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-C" is 0

  @bdd
  Scenario: Quantity and cases are mutually exclusive and an owner must be well formed
    When I Reserve 1 full case and 5 units of SKU "SKU-C" for demand "D-1" for owner "seller-a"
    Then the response status is 400
    And the problem detail type is "quantity-and-cases-exclusive"
    When I Reserve 5 units of SKU "SKU-C" for demand "D-2" for owner "no good"
    Then the response status is 400
    And the problem detail type is "invalid-owner-ref"
