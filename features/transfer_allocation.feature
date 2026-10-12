# Origin-side allocation of stock for an inter-site transfer line.
#
# Derived from:
#   - docs/docs/adr/0031-*: network-fulfillment commands the origin site to
#     allocate stock per transfer line over Kafka; the decision is recorded in
#     a ledger keyed by transfer_line_id and answered with TransferStockAllocated
#     or TransferStockAllocationRejected
#   - rejection reasons: ORIGIN_SITE_UNKNOWN (no stock units recorded at the
#     origin site for the SKU), INSUFFICIENT_USABLE (the request exceeds the
#     origin site's usable total), IDEMPOTENCY_CONFLICT (the same
#     transfer_line_id arrives with a different payload)
#   - a replay of the identical command changes nothing and announces nothing
#   - a malformed command (empty ids, non-positive quantity) is refused before
#     any decision is recorded
# The command arrives over Kafka, so it is driven through the real use case;
# the outcome is asserted from the published events and the REST read model.

Feature: Allocating origin-site stock for a transfer line
  As the network transfer saga
  I want an origin site to promise stock to a transfer line exactly once
  So that the same units are never promised to two transfers

  Background:
    Given an empty warehouse

  @bdd
  Scenario: Stock at the origin site is reserved and the allocation is announced
    Given site "SITE-ORIG" holds 10 units of SKU "SKU-A1" in Bin "BIN-O1"
    When an allocation of 4 units of SKU "SKU-A1" from Site "SITE-ORIG" is requested for transfer "tr-1" line "tl-1"
    Then the allocation command is processed
    And exactly 1 "TransferStockAllocated" domain event is published
    And exactly 0 "TransferStockAllocationRejected" domain events are published
    And the allocation reserved "BIN-O1=4"
    And the Usable inventory for SKU "SKU-A1" is 6

  @bdd
  Scenario: An allocation of everything the origin site holds draws from every bin
    Given site "SITE-ORIG" holds 3 units of SKU "SKU-A2" in Bin "BIN-O1"
    And site "SITE-ORIG" holds 2 units of SKU "SKU-A2" in Bin "BIN-O2"
    When an allocation of 5 units of SKU "SKU-A2" from Site "SITE-ORIG" is requested for transfer "tr-2" line "tl-2"
    Then the allocation reserved "BIN-O1=3,BIN-O2=2"
    And the Usable inventory for SKU "SKU-A2" is 0

  @bdd
  Scenario: Asking for more than the origin site has usable is rejected without reserving anything
    Given site "SITE-ORIG" holds 3 units of SKU "SKU-A3" in Bin "BIN-O1"
    When an allocation of 4 units of SKU "SKU-A3" from Site "SITE-ORIG" is requested for transfer "tr-3" line "tl-3"
    Then the allocation command is processed
    And the transfer allocation was rejected with reason "INSUFFICIENT_USABLE"
    And exactly 0 "TransferStockAllocated" domain events are published
    And the Usable inventory for SKU "SKU-A3" is 3

  @bdd
  Scenario: A site with no recorded stock of the SKU is rejected as an unknown origin
    Given site "SITE-OTHER" holds 9 units of SKU "SKU-A4" in Bin "BIN-X1"
    When an allocation of 1 unit of SKU "SKU-A4" from Site "SITE-ORIG" is requested for transfer "tr-4" line "tl-4"
    Then the transfer allocation was rejected with reason "ORIGIN_SITE_UNKNOWN"
    And the Usable inventory for SKU "SKU-A4" is 9

  @bdd
  Scenario: Stock held by another site is never drawn on
    Given site "SITE-ORIG" holds 2 units of SKU "SKU-A5" in Bin "BIN-O1"
    And site "SITE-OTHER" holds 20 units of SKU "SKU-A5" in Bin "BIN-X1"
    When an allocation of 5 units of SKU "SKU-A5" from Site "SITE-ORIG" is requested for transfer "tr-5" line "tl-5"
    Then the transfer allocation was rejected with reason "INSUFFICIENT_USABLE"
    And the Usable inventory for SKU "SKU-A5" is 22

  @bdd
  Scenario: Already reserved stock is not available to a transfer
    Given site "SITE-ORIG" holds 5 units of SKU "SKU-A6" in Bin "BIN-O1"
    And a Reservation of 4 units of SKU "SKU-A6" for demand "order-1"
    When an allocation of 2 units of SKU "SKU-A6" from Site "SITE-ORIG" is requested for transfer "tr-6" line "tl-6"
    Then the transfer allocation was rejected with reason "INSUFFICIENT_USABLE"

  @bdd
  Scenario: Replaying the identical command changes nothing and announces nothing new
    Given site "SITE-ORIG" holds 10 units of SKU "SKU-A7" in Bin "BIN-O1"
    And an allocation of 4 units of SKU "SKU-A7" from Site "SITE-ORIG" is requested for transfer "tr-7" line "tl-7"
    When an allocation of 4 units of SKU "SKU-A7" from Site "SITE-ORIG" is requested for transfer "tr-7" line "tl-7"
    Then the allocation command is processed
    And exactly 1 "TransferStockAllocated" domain event is published
    And exactly 0 "TransferStockAllocationRejected" domain events are published
    And the Usable inventory for SKU "SKU-A7" is 6

  @bdd
  Scenario: The same line with a different quantity is an idempotency conflict
    Given site "SITE-ORIG" holds 10 units of SKU "SKU-A8" in Bin "BIN-O1"
    And an allocation of 4 units of SKU "SKU-A8" from Site "SITE-ORIG" is requested for transfer "tr-8" line "tl-8"
    When an allocation of 6 units of SKU "SKU-A8" from Site "SITE-ORIG" is requested for transfer "tr-8" line "tl-8"
    Then the transfer allocation was rejected with reason "IDEMPOTENCY_CONFLICT"
    And the Usable inventory for SKU "SKU-A8" is 6

  @bdd
  Scenario: The same line under a different transfer id is an idempotency conflict
    Given site "SITE-ORIG" holds 10 units of SKU "SKU-A9" in Bin "BIN-O1"
    And an allocation of 4 units of SKU "SKU-A9" from Site "SITE-ORIG" is requested for transfer "tr-9" line "tl-9"
    When an allocation of 4 units of SKU "SKU-A9" from Site "SITE-ORIG" is requested for transfer "tr-OTHER" line "tl-9"
    Then the transfer allocation was rejected with reason "IDEMPOTENCY_CONFLICT"

  @bdd
  Scenario Outline: A malformed allocation command is refused and decides nothing
    Given site "SITE-ORIG" holds 10 units of SKU "SKU-B1" in Bin "BIN-O1"
    When an allocation of <qty> units of SKU "<sku>" from Site "<site>" is requested for transfer "<transfer>" line "<line>"
    Then the allocation command is refused as malformed
    And exactly 0 "TransferStockAllocated" domain events are published
    And exactly 0 "TransferStockAllocationRejected" domain events are published
    And the Usable inventory for SKU "SKU-B1" is 10

    Examples:
      | qty | sku    | site      | transfer | line  |
      | 0   | SKU-B1 | SITE-ORIG | tr-1     | tl-1  |
      | -2  | SKU-B1 | SITE-ORIG | tr-1     | tl-1  |
      | 3   |        | SITE-ORIG | tr-1     | tl-1  |
      | 3   | SKU-B1 |           | tr-1     | tl-1  |
      | 3   | SKU-B1 | SITE-ORIG |          | tl-1  |
      | 3   | SKU-B1 | SITE-ORIG | tr-1     |       |
