# Stowing a staged transfer receipt into destination bins.
#
# Derived from:
#   - apis/openapi.yaml POST /transfers/{transferLineId}/stow (ADR 0031): the
#     ONLY path that raises the destination site's usable stock, exactly once
#     per transfer line; one StockUnit per bin leg is created AT the
#     destination site, the receipt moves STAGED -> STOWED and
#     TransferStockStowed is published
#       200 stowed (or an idempotent replay with the ORIGINAL allocations,
#           replay: true, no second StockUnit, nothing new published)
#       400 bins missing/empty, a binId empty, a quantity not positive
#       409 receipt-not-staged (stow before stage, or unknown line);
#           bin-wrong-site (a bin outside the destination site's custody,
#           including a site-less legacy bin); stow-quantity-mismatch (the bin
#           quantities must sum EXACTLY to receivedQuantity)
#   - an unknown bin id is bin-not-found

Feature: Stowing received transfer goods
  As a destination site
  I want to place staged goods into my own bins
  So that the stock becomes usable once and only once

  Background:
    Given an empty warehouse
    And site "SITE-ORIG" holds 10 units of SKU "SKU-S1" in Bin "BIN-O1"
    And an allocation of 5 units of SKU "SKU-S1" from Site "SITE-ORIG" is requested for transfer "tr-1" line "tl-1"
    And Bin "BIN-D1" with capacity 10 belongs to site "SITE-DEST"
    And Bin "BIN-D2" with capacity 10 belongs to site "SITE-DEST"
    And Bin "BIN-SMALL" with capacity 2 belongs to site "SITE-DEST"
    And Bin "BIN-ELSE" with capacity 10 belongs to site "SITE-ELSE"
    And a Bin "BIN-LEGACY" with capacity 10

  @bdd
  Scenario: Stowing across several bins creates one allocation per bin
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    When the receipt for line "tl-1" is stowed into "BIN-D1=3,BIN-D2=2"
    Then the response status is 200
    And the stow places 5 units across 2 allocations
    And the stow allocations are "BIN-D1=3,BIN-D2=2"
    And Bin "BIN-D1" reports 3 occupied
    And Bin "BIN-D2" reports 2 occupied

  @bdd
  Scenario: The stowed receipt reports the staged variance
    Given the destination Site "SITE-DEST" scans 3 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    When the receipt for line "tl-1" is stowed into "BIN-D1=3"
    Then the response status is 200
    And the transfer receipt is STOWED with expected 5, received 3 and variance -2

  @bdd
  Scenario: A replayed stow ignores a different bin choice and keeps the original
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    And the receipt for line "tl-1" is stowed into "BIN-D1=5"
    When the receipt for line "tl-1" is stowed into "BIN-D2=5"
    Then the response status is 200
    And the transfer receipt response is a replay
    And the stow allocations are "BIN-D1=5"
    And Bin "BIN-D2" reports 0 occupied

  @bdd
  Scenario: Stowing an unknown line is a receipt-not-staged problem
    When the receipt for line "tl-nowhere" is stowed into "BIN-D1=5"
    Then the response status is 409
    And the problem detail type is "receipt-not-staged"

  @bdd
  Scenario: A quarantined scan leaves nothing to stow
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-WRONG" line "tl-1"
    When the receipt for line "tl-1" is stowed into "BIN-D1=5"
    Then the response status is 409
    And the problem detail type is "receipt-not-staged"

  @bdd
  Scenario Outline: A bin outside the destination site's custody is refused and nothing is stowed
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    When the receipt for line "tl-1" is stowed into "<bins>"
    Then the response status is 409
    And the problem detail type is "bin-wrong-site"
    And Bin "BIN-D1" reports 0 occupied
    And exactly 0 "TransferStockStowed" domain events are published
    And the Usable inventory for SKU "SKU-S1" is 5

    Examples:
      | bins                 |
      | BIN-ELSE=5           |
      | BIN-LEGACY=5         |
      | BIN-D1=2,BIN-ELSE=3  |

  @bdd
  Scenario Outline: Bin quantities must sum exactly to what was received
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    When the receipt for line "tl-1" is stowed into "<bins>"
    Then the response status is 409
    And the problem detail type is "stow-quantity-mismatch"
    And Bin "BIN-D1" reports 0 occupied
    And exactly 0 "TransferStockStowed" domain events are published

    Examples:
      | bins             |
      | BIN-D1=4         |
      | BIN-D1=6         |
      | BIN-D1=2,BIN-D2=2 |

  @bdd
  Scenario: A stow against a bin that does not exist is bin-not-found
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    When the receipt for line "tl-1" is stowed into "BIN-GHOST=5"
    Then the response status is 404
    And the problem detail type is "bin-not-found"

  @bdd
  Scenario: A failed stow can be retried with correct bins
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    And the receipt for line "tl-1" is stowed into "BIN-D1=4"
    When the receipt for line "tl-1" is stowed into "BIN-D1=5"
    Then the response status is 200
    And the transfer receipt response is not a replay
    And Bin "BIN-D1" reports 5 occupied

  @bdd
  Scenario Outline: A malformed stow request is rejected as a bad request
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    When a POST is sent to "/transfers/tl-1/stow" with the body <body>
    Then the response status is 400
    And exactly 0 "TransferStockStowed" domain events are published

    Examples:
      | body                                        |
      | {}                                          |
      | {"bins":[]}                                 |
      | {"bins":[{"quantity":5}]}                   |
      | {"bins":[{"binId":"","quantity":5}]}        |
      | {"bins":[{"binId":"BIN-D1"                  |

  @bdd
  Scenario: An empty bins list names the missing placement
    When a POST is sent to "/transfers/tl-1/stow" with the body {"bins":[]}
    Then the problem detail type is "bins-required"

  @bdd
  Scenario Outline: A non-positive bin quantity is a bad request
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    When a POST is sent to "/transfers/tl-1/stow" with the body {"bins":[{"binId":"BIN-D1","quantity":<qty>}]}
    Then the response status is 400
    And exactly 0 "TransferStockStowed" domain events are published

    Examples:
      | qty |
      | 0   |
      | -3  |

  @bdd
  Scenario: A non-positive bin quantity names the quantity
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-S1" for transfer "tr-1" line "tl-1"
    When a POST is sent to "/transfers/tl-1/stow" with the body {"bins":[{"binId":"BIN-D1","quantity":0}]}
    Then the response status is 400
    And the problem detail type is "quantity-not-positive"
