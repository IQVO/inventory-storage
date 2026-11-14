# Documentation-derived rules for the storage use cases #1-#7, #9-#11 that no
# earlier scenario pins: every "Fails when" row of Receive/Stow/Reserve, the
# documented boundaries (bin capacity, reservation timeout, bin capacity range,
# page limits), the allocation draw order (ADR 0050), the Reservation and
# StockUnit state-machine edges, and the "does nothing" rules.
#
# Derived from (warehouse-docs contexts/inventory-storage and the in-repo copies):
#   - use-cases.md #1 ReceiveStock, #2 StowStock (placement, fail-open and
#     fail-closed rules), #3 ReserveStock (replay guard, forward-first draw),
#     #4 RevokeReservation, #5 ConfirmPick, #6 GetUsable ("never 404s"),
#     #7 RunCycleCount, #9 GetReservationsByDemandRef, #10 RegisterBin.
#   - aggregate-design-canvas.md: StockUnit S4-S7 and its state transitions,
#     Bin B1-B6 and its derived states, Reservation R3-R6.
#   - troubleshooting.md "REST problem types": invalid-limit / invalid-cursor
#     (limit 1..500), capacity-out-of-range, empty-sku, empty-bin-id,
#     zero-quantity, missing-demand-ref, reservation-already-resolved.

Feature: Storage rules from the documentation
  Chaotic storage keeps exactly one invariant per aggregate; every refused
  command leaves the ledger, the bin occupancy and the event stream untouched.

  Background:
    Given an empty warehouse

  # ---------------------------------------------------------------- receive --

  @bdd
  Scenario Outline: A receipt needs an item scan and a positive quantity and stages nothing otherwise
    When a POST is sent to "/stock-receipts" with the body <body>
    Then the response status is <status>
    And the problem detail type is "<slug>"
    And the domain event "StockReceived" was not published

    Examples:
      | body                              | status | slug          |
      | {"sku":"","quantity":5}           | 400    | empty-sku     |
      | {"sku":"SKU-RC","quantity":0}     | 422    | zero-quantity |
      | {"sku":"SKU-RC","quantity":-4}    | 422    | zero-quantity |

  # ------------------------------------------------------------------- stow --

  @bdd
  Scenario Outline: A stow needs both scans and a positive quantity and a refused stow never touches the bin
    Given a Bin "ST-1" with capacity 10
    When a POST is sent to "/stowages" with the body <body>
    Then the response status is <status>
    And the problem detail type is "<slug>"
    And Bin "ST-1" reports 0 occupied
    And the domain event "ItemStowed" was not published

    Examples:
      | body                                        | status | slug          |
      | {"sku":"","quantity":2,"binId":"ST-1"}      | 400    | empty-sku     |
      | {"sku":"SKU-ST","quantity":2,"binId":""}    | 400    | empty-bin-id  |
      | {"sku":"SKU-ST","quantity":0,"binId":"ST-1"} | 422   | zero-quantity |

  @bdd
  Scenario Outline: A stow fits up to the bin's last free unit and is refused one unit beyond
    Given a Bin "ST-2" with capacity 10
    And 6 units of SKU "SKU-ST" are Stowed into Bin "ST-2"
    When I Stow <qty> units of SKU "SKU-ST" into Bin "ST-2"
    Then the response status is <status>
    And Bin "ST-2" reports <occupied> occupied
    And the domain event "ItemStowed" was published exactly <events> times
    And the Usable inventory for SKU "SKU-ST" is <occupied>

    Examples:
      | qty | status | occupied | events |
      | 4   | 201    | 10       | 2      |
      | 5   | 409    | 6        | 1      |

  @bdd
  Scenario: Placement fails open for a bin facility-layout has no record of and for a SKU the lookup rules do not concern
    Given a Bin "FO-1" with capacity 10
    And a Bin "FO-2" with capacity 10
    And SKU "SKU-HZ" is classified by product-master as Hazmat with DOT hazard class 3
    And product-master sends classification event "pm-fr-1" for SKU "SKU-FR" with tags "Fragile, HighValue", temperature class "", DOT class 0 and version 1
    When I Stow 2 units of SKU "SKU-HZ" into Bin "FO-1"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-HZ" is 2
    Given the facility-layout classification lookup is unavailable
    When I Stow 2 units of SKU "SKU-FR" into Bin "FO-2"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-FR" is 2

  @bdd
  Scenario: An unclassified occupant or one without a DOT class never blocks a DOT-classified stow
    Given a Bin "DO-1" with capacity 20
    And Bin "DO-1" sits in a hazmat-rated zone
    And SKU "SKU-HZ0" is classified by product-master as Hazmat with DOT hazard class 0
    And SKU "SKU-FLAM" is classified by product-master as Hazmat with DOT hazard class 3
    And 4 units of SKU "SKU-OLD" are Stowed into Bin "DO-1"
    And 3 units of SKU "SKU-HZ0" are Stowed into Bin "DO-1"
    When I Stow 2 units of SKU "SKU-FLAM" into Bin "DO-1"
    Then the response status is 201
    And Bin "DO-1" reports 9 occupied

  @bdd
  Scenario: Re-classifying a SKU does not re-check the bins it already occupies
    Given a Bin "RC-1" with capacity 20
    And Bin "RC-1" sits in a non-hazmat "Ambient" zone
    And 5 units of SKU "SKU-RC2" are Stowed into Bin "RC-1"
    And SKU "SKU-RC2" is classified by product-master as Hazmat with DOT hazard class 3
    Then the Usable inventory for SKU "SKU-RC2" is 5
    When I Stow 1 unit of SKU "SKU-RC2" into Bin "RC-1"
    Then the response status is 409
    And the problem detail type is "hazmat-zone-required"
    And the Usable inventory for SKU "SKU-RC2" is 5

  # ---------------------------------------------------------------- reserve --

  @bdd
  Scenario Outline: A reservation needs a positive quantity and a demandRef and reserves nothing otherwise
    Given a Bin "RS-0" with capacity 20
    And 10 units of SKU "SKU-R" are Stowed into Bin "RS-0"
    When a POST is sent to "/reservations" with the body <body>
    Then the response status is <status>
    And the problem detail type is "<slug>"
    And the domain event "StockReserved" was not published
    And the Usable inventory for SKU "SKU-R" is 10

    Examples:
      | body                                                | status | slug               |
      | {"sku":"SKU-R","quantity":0,"demandRef":"ORDER-Z"}  | 422    | zero-quantity      |
      | {"sku":"SKU-R","quantity":3,"demandRef":""}         | 400    | missing-demand-ref |
      | {"sku":"SKU-R","quantity":3}                        | 400    | missing-demand-ref |

  @bdd
  Scenario: Reserving exactly the usable quantity succeeds, one more is refused, and a SKU never stocked has nothing to reserve
    Given a Bin "RS-1" with capacity 20
    And 10 units of SKU "SKU-R" are Stowed into Bin "RS-1"
    When I Reserve 10 units of SKU "SKU-R" for demand "ORDER-ALL"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-R" is 0
    When I Reserve 1 unit of SKU "SKU-R" for demand "ORDER-MORE"
    Then the response status is 409
    And the problem detail type is "insufficient-usable"
    When I Reserve 1 unit of SKU "SKU-NEVER" for demand "ORDER-NONE"
    Then the response status is 409
    And the problem detail type is "insufficient-usable"

  @bdd
  Scenario: The same demandRef with another quantity is another order line, never a retry
    Given a Bin "RS-2" with capacity 20
    And 10 units of SKU "SKU-R" are Stowed into Bin "RS-2"
    And a Reservation of 4 units of SKU "SKU-R" for demand "ORDER-X"
    When I Reserve 5 units of SKU "SKU-R" for demand "ORDER-X"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-R" is 1
    When I look up the Reservations for demand "ORDER-X"
    Then the Reservations list contains 2 entries

  @bdd
  Scenario: The draw takes the pick face first, then the smaller usable bin, then the lower bin id, and spans bins
    Given a Bin "PF-1" with capacity 20
    And a Bin "RV-1" with capacity 20
    And 8 units of SKU "SKU-FF" are Stowed into Bin "PF-1"
    And 3 units of SKU "SKU-FF" are Stowed into Bin "RV-1"
    And Bin "PF-1" is the pick face of SKU "SKU-FF"
    And a Bin "RV-C" with capacity 20
    And a Bin "RV-D" with capacity 20
    And a Bin "RV-F" with capacity 20
    And a Bin "RV-E" with capacity 20
    And 8 units of SKU "SKU-SM" are Stowed into Bin "RV-C"
    And 3 units of SKU "SKU-SM" are Stowed into Bin "RV-D"
    And 4 units of SKU "SKU-TIE" are Stowed into Bin "RV-F"
    And 4 units of SKU "SKU-TIE" are Stowed into Bin "RV-E"
    When I Reserve 3 units of SKU "SKU-FF" for demand "ORDER-FF"
    Then the response status is 201
    And the Reservation allocations are "PF-1=3"
    When I Reserve 3 units of SKU "SKU-SM" for demand "ORDER-SM1"
    Then the Reservation allocations are "RV-D=3"
    When I Reserve 4 units of SKU "SKU-SM" for demand "ORDER-SM2"
    Then the Reservation allocations are "RV-C=4"
    When I Reserve 2 units of SKU "SKU-TIE" for demand "ORDER-TIE"
    Then the Reservation allocations are "RV-E=2"

  @bdd
  Scenario: A resolved reservation takes no second resolution and its quantity is released only once
    Given a Bin "RS-3" with capacity 20
    And 10 units of SKU "SKU-R" are Stowed into Bin "RS-3"
    And a Reservation of 4 units of SKU "SKU-R" for demand "ORDER-RES"
    When I Revoke the Reservation
    Then the response status is 204
    When I Revoke the Reservation
    Then the response status is 409
    And the problem detail type is "reservation-already-resolved"
    When I Confirm the pick for the Reservation
    Then the response status is 409
    And the problem detail type is "reservation-already-resolved"
    And the domain event "ReservationRevoked" was published exactly 1 time
    And the domain event "StockPicked" was not published
    And the Usable inventory for SKU "SKU-R" is 10
    Given a Reservation of 4 units of SKU "SKU-R" for demand "ORDER-RES2"
    When I Confirm the pick for the Reservation
    And I Confirm the pick for the Reservation
    Then the response status is 409
    And the problem detail type is "reservation-already-resolved"
    And the domain event "StockPicked" was published exactly 1 time
    And the Usable inventory for SKU "SKU-R" is 6

  @bdd
  Scenario: A reservation inside its 30 minutes is still confirmable
    Given a Bin "RS-4" with capacity 20
    And 10 units of SKU "SKU-R" are Stowed into Bin "RS-4"
    And a Reservation of 4 units of SKU "SKU-R" for demand "ORDER-INSIDE"
    And time advances by 29 minutes
    When I Confirm the pick for the Reservation
    Then the response status is 204
    And the domain event "StockPicked" was published exactly 1 time
    And the domain event "ReservationExpired" was not published

  @bdd
  Scenario: A timed-out reservation is expired exactly once however often it is read
    Given a Bin "RS-5" with capacity 20
    And 10 units of SKU "SKU-R" are Stowed into Bin "RS-5"
    And a Reservation of 4 units of SKU "SKU-R" for demand "ORDER-ONCE"
    And time advances by 31 minutes
    When I look up the Reservations for demand "ORDER-ONCE"
    And I look up the Reservations for demand "ORDER-ONCE"
    Then the Reservations list contains a Reservation with status "EXPIRED"
    And the domain event "ReservationExpired" was published exactly 1 time
    And the Usable inventory for SKU "SKU-R" is 10

  @bdd
  Scenario: A confirmed pick frees the bin's physical capacity for the next stow
    Given a Bin "RS-6" with capacity 10
    And 10 units of SKU "SKU-R" are Stowed into Bin "RS-6"
    And a Reservation of 10 units of SKU "SKU-R" for demand "ORDER-FULL"
    When I Stow 1 unit of SKU "SKU-R" into Bin "RS-6"
    Then the response status is 409
    And the problem detail type is "bin-full"
    When I Confirm the pick for the Reservation
    And I Stow 10 units of SKU "SKU-R" into Bin "RS-6"
    Then the response status is 201
    And Bin "RS-6" reports 10 occupied
    And the Usable inventory for SKU "SKU-R" is 10

  # ---------------------------------------------------------- read models ----

  @bdd
  Scenario: A SKU with no stock has zero usable and is never a 404
    When I request the Usable inventory for SKU "SKU-NEVER-SEEN"
    Then the response status is 200
    And the Usable inventory response reports SKU "SKU-NEVER-SEEN" with 0 usable

  # ------------------------------------------------------------ cycle count --

  @bdd
  Scenario: A shortfall unlocates a unit that is still reserved, which stops counting as usable
    Given a Bin "CC-1" with capacity 20
    And 8 units of SKU "SKU-CC" are Stowed into Bin "CC-1"
    And a Reservation of 4 units of SKU "SKU-CC" for demand "ORDER-CC"
    When I run a Cycle count on Bin "CC-1" with counted quantity 5
    Then the Cycle count reports system quantity 8 and counted quantity 5
    And the domain event "ItemUnlocated" was published exactly 1 time
    And the Usable inventory for SKU "SKU-CC" is 0
    When I Reserve 1 unit of SKU "SKU-CC" for demand "ORDER-CC2"
    Then the response status is 409
    And the problem detail type is "insufficient-usable"

  @bdd
  Scenario: A count of nothing unlocates every unit in the bin
    Given a Bin "CC-2" with capacity 20
    And 2 units of SKU "SKU-CC" are Stowed into Bin "CC-2"
    And 2 units of SKU "SKU-CC" are Stowed into Bin "CC-2"
    When I run a Cycle count on Bin "CC-2" with counted quantity 0
    Then the response status is 200
    And the Cycle count reports system quantity 4 and counted quantity 0
    And the Cycle count reports a discrepancy
    And the domain event "ItemUnlocated" was published exactly 2 times
    And the Usable inventory for SKU "SKU-CC" is 0

  # -------------------------------------------------------------------- bin --

  @bdd
  Scenario: A bin capacity is accepted up to int32 and refused beyond, and registration raises no domain event
    When I register Bin "BG-1" with capacity 2147483647
    Then the response status is 201
    When I register Bin "BG-2" with capacity 2147483648
    Then the response status is 422
    And the problem detail type is "capacity-out-of-range"
    When I look up Bin "BG-2"
    Then the response status is 404
    When I register Bin "BG-1" with capacity 10
    Then the response status is 200
    And no domain events were published

  @bdd
  Scenario: A full bin that is resized up accepts the next stow
    Given I register Bin "BG-3" with capacity 5
    And 5 units of SKU "SKU-BG" are Stowed into Bin "BG-3"
    When I Stow 1 unit of SKU "SKU-BG" into Bin "BG-3"
    Then the response status is 409
    When I register Bin "BG-3" with capacity 8
    Then the response status is 200
    When I Stow 3 units of SKU "SKU-BG" into Bin "BG-3"
    Then the response status is 201
    When I look up Bin "BG-3"
    Then the Bin response reports capacity 8, occupied 8 and available 0

  # ------------------------------------------------------------------ paging --

  @bdd
  Scenario Outline: A page limit outside 1..500 and a cursor this API never issued are refused
    Given a Bin "PG-1" with capacity 20
    And 10 units of SKU "SKU-PG" are Stowed into Bin "PG-1"
    And a Reservation of 1 unit of SKU "SKU-PG" for demand "ORDER-PG" on line 1
    When a GET request is sent to "/reservations?demandRef=ORDER-PG&<query>"
    Then the response status is 400
    And the problem detail type is "<slug>"

    Examples:
      | query           | slug           |
      | limit=0         | invalid-limit  |
      | limit=-1        | invalid-limit  |
      | limit=501       | invalid-limit  |
      | limit=ten       | invalid-limit  |
      | limit=          | invalid-limit  |
      | cursor=garbage  | invalid-cursor |

  @bdd
  Scenario Outline: The smallest and the largest page limit are accepted
    Given a Bin "PG-2" with capacity 20
    And 10 units of SKU "SKU-PG" are Stowed into Bin "PG-2"
    And a Reservation of 1 unit of SKU "SKU-PG" for demand "ORDER-PG2" on line 1
    When a GET request is sent to "/reservations?demandRef=ORDER-PG2&limit=<limit>"
    Then the response status is 200
    And the response lists 1 reservation and no next cursor

    Examples:
      | limit |
      | 1     |
      | 500   |

  @bdd
  Scenario: Pages walk a demandRef's reservations and the last page, even when it exactly fills the limit, has no cursor
    Given a Bin "PG-3" with capacity 20
    And 10 units of SKU "SKU-PG" are Stowed into Bin "PG-3"
    And a Reservation of 1 unit of SKU "SKU-PG" for demand "ORDER-PG3" on line 1
    And a Reservation of 1 unit of SKU "SKU-PG" for demand "ORDER-PG3" on line 2
    And a Reservation of 1 unit of SKU "SKU-PG" for demand "ORDER-PG3" on line 3
    When a GET request is sent to "/reservations?demandRef=ORDER-PG3&limit=2"
    Then the response lists 2 reservations and a next cursor
    When I follow the next cursor
    Then the response lists 1 reservation and no next cursor
    When a GET request is sent to "/reservations?demandRef=ORDER-PG3&limit=3"
    Then the response lists 3 reservations and no next cursor
