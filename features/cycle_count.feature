Feature: Cycle count
  A Cycle count verifies a Bin's contents against system records and
  reconciles discrepancies. A shortfall means the physical item is no longer
  known to be at that Bin, so it is flagged Unlocated.

  Background:
    Given an empty warehouse
    And a Bin "C-1-1" with capacity 20
    And 8 units of SKU "SKU-C" are Stowed into Bin "C-1-1"

  @bdd
  Scenario: A cycle count matching expected quantity reports no discrepancy
    When I run a Cycle count on Bin "C-1-1" with counted quantity 8
    Then the response status is 200
    And the Cycle count reports system quantity 8 and counted quantity 8
    And the Cycle count reports no discrepancy
    And the domain event "CycleCountCompleted" was published
    And the Usable inventory for SKU "SKU-C" is 8

  @bdd
  Scenario: A cycle count mismatch is flagged as a discrepancy
    When I run a Cycle count on Bin "C-1-1" with counted quantity 5
    Then the response status is 200
    And the Cycle count reports system quantity 8 and counted quantity 5
    And the Cycle count reports a discrepancy
    And the domain event "DiscrepancyDetected" was published
    And the domain event "ItemUnlocated" was published
    And the Usable inventory for SKU "SKU-C" is 0

  # Derived from apis/openapi.yaml — POST /bins/{binId}/cycle-count
  # (runCycleCount): "An overage (counted > system) is reported as a
  # DiscrepancyDetected/CycleCountCompleted(discrepancy=true) pair for a
  # separate receiving/audit process to reconcile — this endpoint does not
  # itself create the extra stock." Also .claude/rules/domain-model.md —
  # "Design notes": only a shortfall flags StockUnits Unlocated; an
  # overage is reported for reconciliation.

  @bdd
  Scenario: A cycle count overage is flagged without marking stock Unlocated
    When I run a Cycle count on Bin "C-1-1" with counted quantity 10
    Then the response status is 200
    And the Cycle count reports system quantity 8 and counted quantity 10
    And the Cycle count reports a discrepancy
    And the domain event "DiscrepancyDetected" was published
    And the domain event "ItemUnlocated" was not published
    And the Usable inventory for SKU "SKU-C" is 8
