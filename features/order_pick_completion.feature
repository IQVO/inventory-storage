# Derived from:
#   - docs/docs/adr/0035-confirm-pick-from-task-completed.md and
#     docs/docs/adr/0036-per-line-confirm-pick.md — fulfillment-execution's
#     TaskCompleted for a PICK task confirms the order's reservations: the
#     line named on the event is confirmed alone; without a line, picks are
#     counted and the order is confirmed on its last pick.
#   - The event arrives from Kafka, so it is seeded through ConfirmPicksForOrder,
#     the use case the consumer calls; assertions go through REST and the
#     published domain events.

Feature: Order pick completion
  A PICK task is per order line, and every one carries the same order
  reference. Confirming a reservation physically removes the stock, so it
  must never happen for a line nobody has picked yet.

  Background:
    Given an empty warehouse
    And a Bin "OP-1" with capacity 30
    And 10 units of SKU "SKU-PA" are Stowed into Bin "OP-1"
    And 10 units of SKU "SKU-PB" are Stowed into Bin "OP-1"

  @bdd
  Scenario: A completed pick for a line confirms only that line's reservation
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-L" on line 1
    And a Reservation of 3 units of SKU "SKU-PB" for demand "ORDER-L" on line 2
    When fulfillment-execution reports PICK task "T-1" completed for order "ORDER-L" line 1
    Then the Reservation for demand "ORDER-L" on line 1 is CONFIRMED
    And the Reservation for demand "ORDER-L" on line 2 is ACTIVE
    And the domain event "StockPicked" was published exactly 1 time
    And the Usable inventory for SKU "SKU-PA" is 8
    And the Usable inventory for SKU "SKU-PB" is 7
    When I look up Bin "OP-1"
    Then the Bin response reports capacity 30, occupied 18 and available 12

  @bdd
  Scenario: Without a line, the order is confirmed only on its last completed pick
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-C"
    And a Reservation of 3 units of SKU "SKU-PB" for demand "ORDER-C"
    When fulfillment-execution reports PICK task "T-1" completed for order "ORDER-C"
    Then every Reservation for demand "ORDER-C" is ACTIVE
    And the domain event "StockPicked" was not published
    When fulfillment-execution reports PICK task "T-2" completed for order "ORDER-C"
    Then every Reservation for demand "ORDER-C" is CONFIRMED
    And the domain event "StockPicked" was published exactly 2 times
    And the Usable inventory for SKU "SKU-PA" is 8
    And the Usable inventory for SKU "SKU-PB" is 7

  @bdd
  Scenario: Non-pick tasks and redelivered events never confirm an order early
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-N"
    And a Reservation of 3 units of SKU "SKU-PB" for demand "ORDER-N"
    When fulfillment-execution reports PACK task "T-9" completed for order "ORDER-N"
    And fulfillment-execution reports PICK task "T-1" completed for order "ORDER-N"
    And fulfillment-execution reports PICK task "T-1" completed for order "ORDER-N"
    Then every Reservation for demand "ORDER-N" is ACTIVE
    And the domain event "StockPicked" was not published
