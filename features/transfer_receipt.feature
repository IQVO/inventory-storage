# Derived from:
#   - docs/docs/adr/0031 / 0030 / 0033 (destination transfer receipt custody,
#     site-scoped allocation) and apis/openapi.yaml — POST
#     /transfers/{transferLineId}/receipt and /stow.
#   - A recognised ALLOCATED line stages a receipt (201) and moves NO usable
#     stock; only a stow raises the destination's usable, exactly once per
#     transfer line. An unrecognised scan is quarantined (422), never stock.
#   - The origin side is a Kafka command: transfer-management's allocation
#     request is seeded through AllocateTransferStock, the use case the
#     consumer calls.
#   - Not covered here: the Idempotency-Key header (ADR 0018) needs the
#     Postgres-backed middleware, which the in-memory harness does not wire.

Feature: Transfer receipt and stow
  A network transfer holds stock at the origin site, then the destination site
  counts what arrived (stage) and places it into its own bins (stow). Staging
  records what was counted against what was expected; stowing is the only
  step that makes the goods usable at the destination.

  Background:
    Given an empty warehouse
    And site "SITE-ORIGIN" holds 10 units of SKU "SKU-T1" in Bin "ORG-1"
    And transfer-management asks site "SITE-ORIGIN" to hold 6 units of SKU "SKU-T1" for line "tl-1" of transfer "tr-1"

  @bdd
  Scenario: Staging a short receipt records the variance and moves no usable stock
    When I stage a receipt for line "tl-1" of transfer "tr-1" at site "SITE-DEST" counting 5 units of SKU "SKU-T1"
    Then the response status is 201
    And the transfer receipt is STAGED with expected 6, received 5 and variance -1
    And the transfer receipt response is not a replay
    And the domain event "TransferReceiptStaged" was published exactly 1 time
    And the Usable inventory for SKU "SKU-T1" is 4

  @bdd
  Scenario: Staging the same scan again returns the original receipt and publishes nothing new
    Given the destination site "SITE-DEST" has counted 5 units of SKU "SKU-T1" for line "tl-1" of transfer "tr-1"
    When I stage a receipt for line "tl-1" of transfer "tr-1" at site "SITE-DEST" counting 5 units of SKU "SKU-T1"
    Then the response status is 200
    And the transfer receipt response is a replay
    And the transfer receipt is STAGED with expected 6, received 5 and variance -1
    And the domain event "TransferReceiptStaged" was published exactly 1 time

  @bdd
  Scenario: A different scan for an already-staged line is refused
    Given the destination site "SITE-DEST" has counted 5 units of SKU "SKU-T1" for line "tl-1" of transfer "tr-1"
    When I stage a receipt for line "tl-1" of transfer "tr-1" at site "SITE-DEST" counting 6 units of SKU "SKU-T1"
    Then the response status is 409
    And the problem detail type is "transfer-receipt-conflict"
    And the domain event "TransferReceiptStaged" was published exactly 1 time

  @bdd
  Scenario: A scan that cannot be matched to an allocated line is quarantined
    When I stage a receipt for line "tl-ghost" of transfer "tr-ghost" at site "SITE-DEST" counting 5 units of SKU "SKU-T1"
    Then the response status is 422
    And the problem detail type is "transfer-scan-quarantined"
    And the quarantine exception is of kind "UNKNOWN_TRANSFER"
    Given transfer-management asks site "SITE-ORIGIN" to hold 50 units of SKU "SKU-T1" for line "tl-big" of transfer "tr-2"
    Then the transfer allocation was rejected with reason "INSUFFICIENT_USABLE"
    When I stage a receipt for line "tl-big" of transfer "tr-2" at site "SITE-DEST" counting 50 units of SKU "SKU-T1"
    Then the response status is 422
    And the problem detail type is "transfer-scan-quarantined"
    And the quarantine exception is of kind "UNRECOGNIZED_TRANSFER"
    And the domain event "TransferReceiptStaged" was not published
    And the Usable inventory for SKU "SKU-T1" is 4

  @bdd
  Scenario: Stowing a line that was never staged is refused
    Given a Bin "DST-1" with capacity 10
    When I stow transfer line "tl-1" into bins:
      | binId | quantity |
      | DST-1 | 5        |
    Then the response status is 409
    And the problem detail type is "receipt-not-staged"

  @bdd
  Scenario: Stowing into a bin outside the destination site's custody is refused
    Given the destination site "SITE-DEST" has counted 5 units of SKU "SKU-T1" for line "tl-1" of transfer "tr-1"
    And Bin "OTHER-1" with capacity 10 belongs to site "SITE-OTHER"
    And a Bin "LEGACY-1" with capacity 10
    When I stow transfer line "tl-1" into bins:
      | binId   | quantity |
      | OTHER-1 | 5        |
    Then the response status is 409
    And the problem detail type is "bin-wrong-site"
    When I stow transfer line "tl-1" into bins:
      | binId    | quantity |
      | LEGACY-1 | 5        |
    Then the response status is 409
    And the problem detail type is "bin-wrong-site"
    And the Usable inventory for SKU "SKU-T1" is 4

  @bdd
  Scenario: Stow quantities that do not sum to the received quantity are refused
    Given the destination site "SITE-DEST" has counted 5 units of SKU "SKU-T1" for line "tl-1" of transfer "tr-1"
    And Bin "DST-1" with capacity 10 belongs to site "SITE-DEST"
    When I stow transfer line "tl-1" into bins:
      | binId | quantity |
      | DST-1 | 3        |
    Then the response status is 409
    And the problem detail type is "stow-quantity-mismatch"
    And the domain event "TransferStockStowed" was not published
    And the Usable inventory for SKU "SKU-T1" is 4

  @bdd
  Scenario: Stowing a staged receipt raises the destination's usable stock
    Given the destination site "SITE-DEST" has counted 5 units of SKU "SKU-T1" for line "tl-1" of transfer "tr-1"
    And Bin "DST-1" with capacity 10 belongs to site "SITE-DEST"
    And Bin "DST-2" with capacity 10 belongs to site "SITE-DEST"
    When I stow transfer line "tl-1" into bins:
      | binId | quantity |
      | DST-1 | 3        |
      | DST-2 | 2        |
    Then the response status is 200
    And the transfer receipt is STOWED with expected 6, received 5 and variance -1
    And the stow places 5 units across 2 allocations
    And the domain event "TransferStockStowed" was published exactly 1 time
    And the Usable inventory for SKU "SKU-T1" is 9
    When I look up Bin "DST-1"
    Then the Bin response reports capacity 10, occupied 3 and available 7

  @bdd
  Scenario: Stowing the same line again returns the original outcome and creates no second stock
    Given the destination site "SITE-DEST" has counted 5 units of SKU "SKU-T1" for line "tl-1" of transfer "tr-1"
    And Bin "DST-1" with capacity 10 belongs to site "SITE-DEST"
    And I stow transfer line "tl-1" into bins:
      | binId | quantity |
      | DST-1 | 5        |
    When I stow transfer line "tl-1" into bins:
      | binId | quantity |
      | DST-1 | 5        |
    Then the response status is 200
    And the transfer receipt response is a replay
    And the stow places 5 units across 1 allocations
    And the domain event "TransferStockStowed" was published exactly 1 time
    And the Usable inventory for SKU "SKU-T1" is 9
