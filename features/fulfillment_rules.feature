# Documentation-derived rules for the TaskCompleted PICK consumer
# (ConfirmPicksForOrder, use case #12) and the ReceiptLineReceived consumer
# (BookInboundReceiptLine, use case #13).
#
# Derived from:
#   - use-cases.md #12: "Does nothing (a successful no-op) when task_type is
#     not exactly PICK, when order_ref is empty ... or when no reservation
#     carries that demand_ref ... No progress row is left for an order that has
#     no reservation"; per-line path (CONFIRMED and REVOKED skipped, EXPIRED
#     skipped without error, nothing counted, "lines 1 and 3 picked confirms 1
#     and 3 and leaves 2 ACTIVE"; no reservation carries the line -> counting
#     over the line-less reservations only, none -> no-op LINE_NOT_FOUND);
#     counting fallback ("an order with one revoked line needs one pick fewer");
#     Fails when: no event id or line_no outside 1..2147483647
#     (ErrMalformedPickCompletion).
#   - use-cases.md #13: no event id, empty SKU, quantity <= 0 or a condition
#     outside Good | Damaged is ErrMalformedInboundReceiptLine (deterministic).
# Both events arrive from Kafka, so they are driven through the use cases the
# consumers call; outcomes are asserted through REST and published events.

Feature: Fulfillment and inbound consumers, documented no-ops and refusals
  A PICK completion physically removes stock, so it must never confirm a line
  nobody picked and never count what it did not handle.

  Background:
    Given an empty warehouse
    And a Bin "FP-1" with capacity 40
    And 10 units of SKU "SKU-PA" are Stowed into Bin "FP-1"
    And 10 units of SKU "SKU-PB" are Stowed into Bin "FP-1"
    And 5 units of SKU "SKU-PC" are Stowed into Bin "FP-1"

  @bdd
  Scenario: Picks for lines 1 and 3 confirm those two lines and leave line 2 active
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-13" on line 1
    And a Reservation of 3 units of SKU "SKU-PB" for demand "ORDER-13" on line 2
    And a Reservation of 1 unit of SKU "SKU-PC" for demand "ORDER-13" on line 3
    When fulfillment-execution reports PICK task "T-1" completed for order "ORDER-13" line 1
    And fulfillment-execution reports PICK task "T-3" completed for order "ORDER-13" line 3
    Then the Reservation for demand "ORDER-13" on line 1 is CONFIRMED
    And the Reservation for demand "ORDER-13" on line 2 is ACTIVE
    And the Reservation for demand "ORDER-13" on line 3 is CONFIRMED
    And the domain event "StockPicked" was published exactly 2 times
    And the Usable inventory for SKU "SKU-PB" is 7

  @bdd
  Scenario: A completion without an order_ref is a no-op and is not counted as a pick
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-E1"
    And a Reservation of 3 units of SKU "SKU-PB" for demand "ORDER-E1"
    When fulfillment-execution reports PICK task "T-0" completed for order ""
    And fulfillment-execution reports PICK task "T-1" completed for order "ORDER-E1"
    Then every Reservation for demand "ORDER-E1" is ACTIVE
    When fulfillment-execution reports PICK task "T-2" completed for order "ORDER-E1"
    Then every Reservation for demand "ORDER-E1" is CONFIRMED
    And the domain event "StockPicked" was published exactly 2 times

  @bdd
  Scenario: A completion for an order that has no reservation leaves no pick count behind
    When fulfillment-execution reports PICK task "T-1" completed for order "ORDER-GHOST"
    And a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-GHOST"
    And a Reservation of 3 units of SKU "SKU-PB" for demand "ORDER-GHOST"
    And fulfillment-execution reports PICK task "T-2" completed for order "ORDER-GHOST"
    Then every Reservation for demand "ORDER-GHOST" is ACTIVE
    And the domain event "StockPicked" was not published

  @bdd
  Scenario: A revoked line means the order needs one pick fewer
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-RV"
    And I Revoke the Reservation
    And a Reservation of 3 units of SKU "SKU-PB" for demand "ORDER-RV"
    When fulfillment-execution reports PICK task "T-1" completed for order "ORDER-RV"
    And I look up the Reservations for demand "ORDER-RV"
    Then the Reservations list contains a Reservation with status "REVOKED"
    And the Reservations list contains a Reservation with status "CONFIRMED"
    And the domain event "StockPicked" was published exactly 1 time
    And the Usable inventory for SKU "SKU-PA" is 10
    And the Usable inventory for SKU "SKU-PB" is 7

  @bdd
  Scenario: A pick for a line whose reservation was revoked confirms nothing and spares the other lines
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-RL" on line 1
    And I Revoke the Reservation
    And a Reservation of 3 units of SKU "SKU-PB" for demand "ORDER-RL" on line 2
    When fulfillment-execution reports PICK task "T-1" completed for order "ORDER-RL" line 1
    Then the Reservation for demand "ORDER-RL" on line 1 is REVOKED
    And the Reservation for demand "ORDER-RL" on line 2 is ACTIVE
    And the domain event "StockPicked" was not published

  @bdd
  Scenario: A pick for a line that timed out is skipped, its stock stays returned and no pick is published
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-TO" on line 1
    And time advances by 31 minutes
    When fulfillment-execution reports PICK task "T-1" completed for order "ORDER-TO" line 1
    Then the Reservation for demand "ORDER-TO" on line 1 is EXPIRED
    And the domain event "ReservationExpired" was published exactly 1 time
    And the domain event "StockPicked" was not published
    And the Usable inventory for SKU "SKU-PA" is 10

  @bdd
  Scenario: A pick naming a line nobody reserved is a no-op when every reservation carries a line
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-NL" on line 1
    When fulfillment-execution reports PICK task "T-1" completed for order "ORDER-NL" line 7
    Then the Reservation for demand "ORDER-NL" on line 1 is ACTIVE
    And the domain event "StockPicked" was not published

  @bdd
  Scenario: A pick naming an unknown line is counted against the line-less reservations only
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-MX"
    And a Reservation of 3 units of SKU "SKU-PB" for demand "ORDER-MX" on line 1
    When fulfillment-execution reports PICK task "T-1" completed for order "ORDER-MX" line 7
    Then the Reservation for demand "ORDER-MX" on line 1 is ACTIVE
    And the domain event "StockPicked" was published exactly 1 time
    And the Usable inventory for SKU "SKU-PA" is 8
    And the Usable inventory for SKU "SKU-PB" is 7

  @bdd
  Scenario Outline: A PICK completion with no event id or a line number outside 1..2147483647 is refused as malformed
    Given a Reservation of 2 units of SKU "SKU-PA" for demand "ORDER-BAD" on line 1
    When fulfillment-execution reports PICK completion with event id "<event>" for order "ORDER-BAD" and line "<line>"
    Then the pick completion is refused as malformed
    And the domain event "StockPicked" was not published
    And the Reservation for demand "ORDER-BAD" on line 1 is ACTIVE

    Examples:
      | event | line       |
      |       | 1          |
      | evt-1 | 0          |
      | evt-1 | -1         |
      | evt-1 | 2147483648 |

  @bdd
  Scenario: The largest 32-bit line number is a valid line
    When fulfillment-execution reports PICK completion with event id "evt-1" for order "ORDER-BIG" and line "2147483647"
    Then the pick completion is processed

  @bdd
  Scenario Outline: An inbound receipt line that breaks a rule is refused as malformed and stages nothing
    When inbound-receiving reports a line with event id "<event>", SKU "<sku>", quantity <qty> and condition "<condition>"
    Then the inbound line is refused as malformed
    And the domain event "StockReceived" was not published
    And the Usable inventory for SKU "SKU-IB" is 0

    Examples:
      | event | sku    | qty | condition | broken rule                        |
      |       | SKU-IB | 5   | Good      | no CloudEvents id                  |
      | evt-1 |        | 5   | Good      | empty SKU                          |
      | evt-1 | SKU-IB | 0   | Good      | quantity of zero                   |
      | evt-1 | SKU-IB | -3  | Good      | negative quantity                  |
      | evt-1 | SKU-IB | 5   | Pristine  | condition outside Good or Damaged  |
      | evt-1 | SKU-IB | 5   | good      | condition is case-sensitive        |
