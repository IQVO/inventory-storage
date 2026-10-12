# Derived from:
#   - apis/openapi.yaml — POST /bins/{binId}/cycle-count: 400
#     counted-quantity-required (an explicit 0 is valid, omitting it is not),
#     422 negative-quantity; a shortfall marks "whichever StockUnits cover the
#     difference" fully Unlocated and publishes ItemUnlocated per unit touched.
#   - .claude/rules/domain-model.md — Unlocated stock is not usable and is no
#     longer part of the bin's system quantity.

Feature: Cycle count edge cases
  Recounting a bin is a normal operational action. What the count may say, and
  what a shortfall does to the stock, is bounded.

  Background:
    Given an empty warehouse
    And a Bin "CE-1" with capacity 20

  @bdd
  Scenario: A counted quantity that is negative or missing is rejected and records nothing
    When I run a Cycle count on Bin "CE-1" with a negative counted quantity of 3
    Then the response status is 422
    And the problem detail type is "negative-quantity"
    When I run a Cycle count on Bin "CE-1" without a counted quantity
    Then the response status is 400
    And the problem detail type is "counted-quantity-required"
    And the domain event "CycleCountCompleted" was not published

  @bdd
  Scenario: A shortfall unlocates only the units that cover it and a recount ignores them
    Given 4 units of SKU "SKU-CE" are Stowed into Bin "CE-1"
    And 4 units of SKU "SKU-CE" are Stowed into Bin "CE-1"
    When I run a Cycle count on Bin "CE-1" with counted quantity 6
    Then the response status is 200
    And the Cycle count reports system quantity 8 and counted quantity 6
    And the Cycle count reports a discrepancy
    And the domain event "ItemUnlocated" was published exactly 1 time
    And the Usable inventory for SKU "SKU-CE" is 4
    When I run a Cycle count on Bin "CE-1" with counted quantity 4
    Then the Cycle count reports system quantity 4 and counted quantity 4
    And the Cycle count reports no discrepancy
