# Documentation-derived edge of use case #16 StowTransferStock.
#
# Derived from use-cases.md #16: bins must belong to the receipt's destination
# site and the quantities must sum exactly to received_quantity; "bin occupancy
# is taken ... all in ONE unit of work", so a leg that overflows its bin
# refuses the stow (bin-full) and the receipt stays STAGED, receivable again by
# a correct stow; a bin filled EXACTLY to its capacity is fine.
# (The harness has no UnitOfWork, so the overflowing leg is placed first: the
# rollback of EARLIER legs is Postgres transaction behaviour, covered by the
# integration suite, not reachable here.)

Feature: A transfer stow that overflows a destination bin
  The destination's stock becomes usable once and only once, in whole.

  Background:
    Given an empty warehouse
    And site "SITE-ORIG" holds 10 units of SKU "SKU-S1" in Bin "BIN-O1"
    And an allocation of 5 units of SKU "SKU-S1" from Site "SITE-ORIG" is requested for transfer "tr-1" line "tl-1"
    And Bin "BIN-D1" with capacity 10 belongs to site "SITE-DEST"
    And Bin "BIN-SMALL" with capacity 2 belongs to site "SITE-DEST"
    And the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"

  @bdd
  Scenario: A leg larger than its bin refuses the stow and leaves the receipt STAGED, a leg that exactly fills its bin is accepted
    When the receipt for line "tl-1" is stowed into "BIN-SMALL=3,BIN-D1=2"
    Then the response status is 409
    And the problem detail type is "bin-full"
    And Bin "BIN-D1" reports 0 occupied
    And Bin "BIN-SMALL" reports 0 occupied
    And exactly 0 "TransferStockStowed" domain events are published
    And the Usable inventory for SKU "SKU-S1" is 5
    When the receipt for line "tl-1" is stowed into "BIN-D1=3,BIN-SMALL=2"
    Then the response status is 200
    And the transfer receipt response is not a replay
    And Bin "BIN-SMALL" reports 2 occupied
    And Bin "BIN-D1" reports 3 occupied
    And exactly 1 "TransferStockStowed" domain events are published
    And the Usable inventory for SKU "SKU-S1" is 10
