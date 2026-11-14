# Documentation-derived rules for the Relocation aggregate (use cases #17-#20).
#
# Derived from (warehouse-docs contexts/inventory-storage and the in-repo copies):
#   - use-cases.md #17 RelocateStock, #18 ApplyRelocationTask, #19 GetRelocation,
#     #20 ApplyReplenishTask; ADR 0039 and ADR 0041.
#   - aggregate-design-canvas.md "Relocation": invariants L1-L8 (same bin,
#     closed reason set with REPLENISHMENT system-set only, positive quantity,
#     sourceRef <= 200 characters, bins must belong to the site, exactly-once,
#     usable covers the move / destination has room, draw in ascending unit id),
#     corrective policies "check before change", "dead-letter the hopeless",
#     "replenishment closes the loop".
#   - troubleshooting.md REST problem types: bin-not-at-site, same-bin,
#     invalid-relocation-reason, source-ref-too-long, relocation-not-found,
#     insufficient-usable ("the source bin's UNRESERVED quantity is too small").
# The two TaskCompleted legs arrive from Kafka, so they are driven through the
# SAME use cases the consumer calls (ApplyRelocationTask, ApplyReplenishTask),
# handed every completion the way the shared consumer dispatches by type; the
# outcome is asserted through REST and the published domain events.

Feature: Intra-site relocation (REST and TaskCompleted)
  A relocation moves a quantity of one SKU from one bin to another bin of the
  SAME site. Site-level on-hand and usable never change; the stock's location
  does. A rejected move changes nothing.

  Background:
    Given an empty warehouse
    And Bin "SP-A1" with capacity 20 belongs to site "SITE-P"
    And Bin "SP-A2" with capacity 10 belongs to site "SITE-P"
    And Bin "SP-TINY" with capacity 2 belongs to site "SITE-P"
    And Bin "SP-B1" with capacity 20 belongs to site "SITE-Q"
    And a Bin "SP-LEGACY" with capacity 10
    And 10 units of SKU "SKU-RL" are Stowed into Bin "SP-A1"

  # ------------------------------------------------------------- REST ------

  @bdd
  Scenario: A relocation moves stock between bins and leaves the site's usable quantity unchanged
    When I Relocate 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" for reason "CONSOLIDATION"
    Then the response status is 201
    And the response has a Location header pointing at the Relocation
    And the Relocation response reports 4 units of SKU "SKU-RL" moved from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" for reason "CONSOLIDATION"
    And the StockRelocated event reports 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" for reason "CONSOLIDATION" with source reference ""
    And Bin "SP-A1" reports 6 occupied
    And Bin "SP-A2" reports 4 occupied
    And the Usable inventory for SKU "SKU-RL" is 10

  @bdd
  Scenario: A relocation without a reason is a MANUAL move and can be read back by id
    When I Relocate 3 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" with source reference "TICKET-4711"
    Then the response status is 201
    When I look up the Relocation
    Then the response status is 200
    And the Relocation response reports 3 units of SKU "SKU-RL" moved from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" for reason "MANUAL"
    And the Relocation response reports source reference "TICKET-4711"

  @bdd
  Scenario Outline: An operator may relocate for SLOTTING, CONSOLIDATION or MANUAL
    When I Relocate 2 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" for reason "<reason>"
    Then the response status is 201
    And the Relocation response reports 2 units of SKU "SKU-RL" moved from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" for reason "<reason>"

    Examples:
      | reason        |
      | SLOTTING      |
      | CONSOLIDATION |
      | MANUAL        |

  @bdd
  Scenario Outline: REPLENISHMENT is system-set and any other reason is refused, moving nothing
    When I Relocate 2 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" for reason "<reason>"
    Then the response status is 422
    And the problem detail type is "invalid-relocation-reason"
    And Bin "SP-A1" reports 10 occupied
    And Bin "SP-A2" reports 0 occupied
    And exactly 0 "StockRelocated" domain events are published

    Examples:
      | reason        |
      | REPLENISHMENT |
      | slotting      |
      | RESTOCK       |

  @bdd
  Scenario Outline: The source reference is accepted up to 200 characters and refused beyond
    When I Relocate 1 unit of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" with a source reference of <length> characters
    Then the response status is <status>
    And Bin "SP-A2" reports <occupied> occupied

    Examples:
      | length | status | occupied |
      | 200    | 201    | 1        |
      | 201    | 422    | 0        |

  @bdd
  Scenario: A source reference over 200 characters is told apart from other validation errors
    When I Relocate 1 unit of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" with a source reference of 201 characters
    Then the problem detail type is "source-ref-too-long"

  @bdd
  Scenario Outline: A move into the same bin, of nothing, or of a negative quantity is refused
    When I Relocate <qty> units of SKU "SKU-RL" from Bin "SP-A1" to Bin "<to>" at site "SITE-P"
    Then the response status is 422
    And the problem detail type is "<slug>"
    And exactly 0 "StockRelocated" domain events are published

    Examples:
      | qty | to    | slug              |
      | 3   | SP-A1 | same-bin          |
      | 0   | SP-A2 | zero-quantity     |
      | -2  | SP-A2 | zero-quantity     |

  @bdd
  Scenario Outline: An unknown bin is a 404 whatever else is wrong with the move
    When I Relocate 3 units of SKU "SKU-RL" from Bin "<from>" to Bin "<to>" at site "SITE-P"
    Then the response status is 404
    And the problem detail type is "bin-not-found"
    And exactly 0 "StockRelocated" domain events are published

    Examples:
      | from     | to       |
      | SP-GHOST | SP-A2    |
      | SP-A1    | SP-GHOST |
      | SP-GHOST | SP-GHOST |

  @bdd
  Scenario Outline: A relocation never crosses sites and a bin with no known site is outside every site
    Given 10 units of SKU "SKU-RL" are Stowed into Bin "SP-B1"
    When I Relocate 2 units of SKU "SKU-RL" from Bin "<from>" to Bin "<to>" at site "SITE-P"
    Then the response status is 409
    And the problem detail type is "bin-not-at-site"
    And exactly 0 "StockRelocated" domain events are published
    And Bin "SP-A1" reports 10 occupied
    And Bin "SP-B1" reports 10 occupied

    Examples:
      | from  | to        |
      | SP-A1 | SP-B1     |
      | SP-A1 | SP-LEGACY |
      | SP-B1 | SP-A2     |

  @bdd
  Scenario: A bin with no recorded custody belongs to the site named by its seven-segment location code
    Given a Bin "S9-A-01-02-03-04-05" with capacity 10
    And a Bin "S9-A-01-02-03-04-06" with capacity 10
    And 5 units of SKU "SKU-RL" are Stowed into Bin "S9-A-01-02-03-04-05"
    When I Relocate 2 units of SKU "SKU-RL" from Bin "S9-A-01-02-03-04-05" to Bin "S9-A-01-02-03-04-06" at site "S8"
    Then the response status is 409
    And the problem detail type is "bin-not-at-site"
    When I Relocate 2 units of SKU "SKU-RL" from Bin "S9-A-01-02-03-04-05" to Bin "S9-A-01-02-03-04-06" at site "S9"
    Then the response status is 201
    And Bin "S9-A-01-02-03-04-06" reports 2 occupied

  @bdd
  Scenario: Reserved stock never moves, only the unreserved quantity does, and the reservation still picks from the source bin
    Given a Reservation of 4 units of SKU "SKU-RL" for demand "ORDER-R1"
    When I Relocate 7 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P"
    Then the response status is 409
    And the problem detail type is "insufficient-usable"
    And Bin "SP-A1" reports 10 occupied
    And Bin "SP-A2" reports 0 occupied
    And exactly 0 "StockRelocated" domain events are published
    When I Relocate 6 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-RL" is 6
    When I Confirm the pick for the Reservation
    Then the response status is 204
    And Bin "SP-A1" reports 0 occupied
    And Bin "SP-A2" reports 6 occupied
    And the Usable inventory for SKU "SKU-RL" is 6

  @bdd
  Scenario: One unit too many for the destination refuses the whole move and records nothing, exactly enough room accepts it
    Given 4 units of SKU "SKU-RL" are Stowed into Bin "SP-A2"
    When I Relocate 7 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P"
    Then the response status is 409
    And the problem detail type is "bin-full"
    And Bin "SP-A1" reports 10 occupied
    And Bin "SP-A2" reports 4 occupied
    And exactly 0 "StockRelocated" domain events are published
    And exactly 0 "BinStockLevelChanged" domain events are published
    When I look up Relocation "rel-1"
    Then the response status is 404
    And the problem detail type is "relocation-not-found"
    When I Relocate 6 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P"
    Then the response status is 201
    And Bin "SP-A2" reports 10 occupied
    And Bin "SP-A1" reports 4 occupied

  @bdd
  Scenario: The draw takes source units in ascending id and a drained unit is REMOVED
    Given 3 units of SKU "SKU-RD" are Stowed into Bin "SP-A1"
    And 5 units of SKU "SKU-RD" are Stowed into Bin "SP-A1"
    When I Relocate 4 units of SKU "SKU-RD" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P"
    Then the response status is 201
    And the Relocation drew source legs "3,1" in order
    And StockUnit "su-2" is REMOVED with quantity 0
    And StockUnit "su-3" is AVAILABLE with quantity 4
    And StockUnit "su-4" is AVAILABLE with quantity 4

  @bdd
  Scenario: A move raises one level event for the source bin and then one for the destination
    When I Relocate 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" for reason "SLOTTING"
    Then the response status is 201
    And the BinStockLevelChanged events published are:
      | bin   | onHand | reserved | reason   |
      | SP-A1 | 6      | 0        | RELOCATE |
      | SP-A2 | 4      | 0        | RELOCATE |

  # ------------------------------------------------------ RELOCATE task ----

  @bdd
  Scenario: A completed RELOCATE task is applied as a SLOTTING move in the source bin's site, once
    When fulfillment-execution reports RELOCATE task "T-1" completed moving 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" with reference "PLAN-77/MOVE-3"
    Then the move completion is APPLIED
    And exactly 1 "StockRelocated" domain events are published
    And the StockRelocated event reports 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" for reason "SLOTTING" with source reference "PLAN-77/MOVE-3"
    And Bin "SP-A1" reports 6 occupied
    And Bin "SP-A2" reports 4 occupied
    When I look up Relocation "rel-1"
    Then the Relocation response reports 4 units of SKU "SKU-RL" moved from Bin "SP-A1" to Bin "SP-A2" at site "SITE-P" for reason "SLOTTING"

  @bdd
  Scenario: A RELOCATE task without a relocation_ref falls back to the task id as the source reference
    When fulfillment-execution reports RELOCATE task "T-2" completed moving 2 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2"
    Then the move completion is APPLIED
    And the StockRelocated event reports 2 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" for reason "SLOTTING" with source reference "T-2"

  @bdd
  Scenario: A redelivered RELOCATE completion is skipped and the stock moves once
    When fulfillment-execution reports RELOCATE task "T-3" completed moving 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2"
    And fulfillment-execution reports RELOCATE task "T-3" completed moving 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2"
    Then the move completion is DUPLICATE
    And exactly 1 "StockRelocated" domain events are published
    And Bin "SP-A1" reports 6 occupied
    And Bin "SP-A2" reports 4 occupied

  @bdd
  Scenario: A task of another type is ignored by both move consumers
    When fulfillment-execution reports PACK task "T-4" completed moving 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2"
    Then the move completion is ignored
    And exactly 0 "StockRelocated" domain events are published
    And Bin "SP-A2" reports 0 occupied

  @bdd
  Scenario Outline: A RELOCATE completion with a missing or invalid field is refused as malformed and moves nothing
    When fulfillment-execution reports RELOCATE completion with event id "<event>", SKU "<sku>", from "<from>", to "<to>", quantity "<qty>" and reference "PLAN-1"
    Then the move completion is refused as malformed
    And exactly 0 "StockRelocated" domain events are published
    And Bin "SP-A1" reports 10 occupied

    Examples:
      | event | sku    | from  | to    | qty | case                   |
      |       | SKU-RL | SP-A1 | SP-A2 | 4   | no CloudEvents id      |
      | evt-5 |        | SP-A1 | SP-A2 | 4   | empty SKU              |
      | evt-5 | SKU-RL |       | SP-A2 | 4   | empty source bin       |
      | evt-5 | SKU-RL | SP-A1 |       | 4   | empty destination bin  |
      | evt-5 | SKU-RL | SP-A1 | SP-A2 |     | missing quantity       |
      | evt-5 | SKU-RL | SP-A1 | SP-A2 | 0   | zero quantity          |
      | evt-5 | SKU-RL | SP-A1 | SP-A2 | -3  | negative quantity      |

  @bdd
  Scenario Outline: A move the ledger deterministically rejects is dead-lettered as malformed, not retried
    When fulfillment-execution reports RELOCATE task "T-6" completed moving <qty> units of SKU "SKU-RL" from Bin "SP-A1" to Bin "<to>"
    Then the move completion is refused as malformed
    And exactly 0 "StockRelocated" domain events are published
    And Bin "SP-A1" reports 10 occupied

    Examples:
      | to       | qty | case                                |
      | SP-GHOST | 4   | unknown bin                         |
      | SP-B1    | 4   | bin of another site                 |
      | SP-A1    | 4   | same bin                            |
      | SP-A2    | 11  | insufficient usable at the source   |
      | SP-TINY  | 3   | destination full                    |

  # ----------------------------------------------------- REPLENISH task ----

  @bdd
  Scenario: A completed REPLENISH task is applied as a REPLENISHMENT move that closes the request
    When fulfillment-execution reports REPLENISH task "T-7" completed moving 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" with reference "REPL-2026-0042"
    Then the move completion is APPLIED
    And exactly 1 "StockRelocated" domain events are published
    And the StockRelocated event reports 4 units of SKU "SKU-RL" from Bin "SP-A1" to Bin "SP-A2" for reason "REPLENISHMENT" with source reference "REPL-2026-0042"
    And the BinStockLevelChanged events published are:
      | bin   | onHand | reserved | reason        |
      | SP-A1 | 6      | 0        | REPLENISHMENT |
      | SP-A2 | 4      | 0        | REPLENISHMENT |

  @bdd
  Scenario: A REPLENISH task needs its request_ref and never falls back to the task id
    When fulfillment-execution reports REPLENISH completion with event id "evt-8", SKU "SKU-RL", from "SP-A1", to "SP-A2", quantity "4" and reference ""
    Then the move completion is refused as malformed
    And exactly 0 "StockRelocated" domain events are published
    And Bin "SP-A2" reports 0 occupied
