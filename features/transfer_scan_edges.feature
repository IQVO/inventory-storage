# Destination-side scan of goods received against a transfer line.
#
# Derived from:
#   - apis/openapi.yaml POST /transfers/{transferLineId}/receipt (ADR 0031):
#       201 the receipt is staged against a recognized ALLOCATED line (NO usable
#           stock moves: only the stow raises it)
#       200 idempotent replay of the exact same scan returns the ORIGINAL
#           receipt with replay: true
#       400 malformed JSON, transferId/sku/destinationSiteId missing or
#           receivedQuantity not positive
#       409 transfer-receipt-conflict: the line already has a receipt for a
#           DIFFERENT scan; the original stands immutable
#       422 transfer-scan-quarantined: the scan cannot be matched to a
#           recognized ALLOCATED line; nothing changes except an
#           inventory-exception row, carried in the `exception` extension
#   - variance = receivedQuantity - expectedQuantity (negative when short)
#   - TransferReceiptStaged is published when staged, never for a quarantine

Feature: Edge cases when scanning received transfer goods
  As a destination site
  I want to count what arrived against the transfer line
  So that shortages and unrecognized scans are visible before any stock is usable

  Background:
    Given an empty warehouse
    And site "SITE-ORIG" holds 10 units of SKU "SKU-R1" in Bin "BIN-O1"
    And an allocation of 5 units of SKU "SKU-R1" from Site "SITE-ORIG" is requested for transfer "tr-1" line "tl-1"

  @bdd
  Scenario: Receiving exactly what was allocated stages the receipt with no variance
    When the destination Site "SITE-DEST" scans 5 units of SKU "SKU-R1" for transfer "tr-1" line "tl-1"
    Then the response status is 201
    And the transfer receipt is STAGED with expected 5, received 5 and variance 0
    And the transfer receipt response is not a replay
    And exactly 1 "TransferReceiptStaged" domain event is published

  @bdd
  Scenario: A short receipt records a negative variance
    When the destination Site "SITE-DEST" scans 3 units of SKU "SKU-R1" for transfer "tr-1" line "tl-1"
    Then the response status is 201
    And the transfer receipt is STAGED with expected 5, received 3 and variance -2

  @bdd
  Scenario: An over receipt records a positive variance
    When the destination Site "SITE-DEST" scans 8 units of SKU "SKU-R1" for transfer "tr-1" line "tl-1"
    Then the response status is 201
    And the transfer receipt is STAGED with expected 5, received 8 and variance 3

  @bdd
  Scenario Outline: A different scan for an already staged line is a conflict and the original stands
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-R1" for transfer "tr-1" line "tl-1"
    When the destination Site "<site>" scans <qty> units of SKU "SKU-R1" for transfer "<transfer>" line "tl-1"
    Then the response status is 409
    And the problem detail type is "transfer-receipt-conflict"
    And exactly 1 "TransferReceiptStaged" domain event is published

    Examples:
      | site       | qty | transfer |
      | SITE-DEST  | 4   | tr-1     |
      | SITE-ELSE  | 5   | tr-1     |
      | SITE-DEST  | 5   | tr-OTHER |

  @bdd
  Scenario: A scan under the wrong transfer id is quarantined as unrecognized
    When the destination Site "SITE-DEST" scans 5 units of SKU "SKU-R1" for transfer "tr-WRONG" line "tl-1"
    Then the response status is 422
    And the problem detail type is "transfer-scan-quarantined"
    And the quarantine exception is of kind "UNRECOGNIZED_TRANSFER"
    And exactly 0 "TransferReceiptStaged" domain events are published

  @bdd
  Scenario: Scanning at the transfer's own origin site is quarantined as unrecognized
    When the destination Site "SITE-ORIG" scans 5 units of SKU "SKU-R1" for transfer "tr-1" line "tl-1"
    Then the response status is 422
    And the quarantine exception is of kind "UNRECOGNIZED_TRANSFER"

  @bdd
  Scenario: A line whose allocation was rejected cannot be received
    Given an allocation of 99 units of SKU "SKU-R1" from Site "SITE-ORIG" is requested for transfer "tr-2" line "tl-2"
    When the destination Site "SITE-DEST" scans 1 unit of SKU "SKU-R1" for transfer "tr-2" line "tl-2"
    Then the response status is 422
    And the quarantine exception is of kind "UNRECOGNIZED_TRANSFER"

  @bdd
  Scenario: A quarantined scan leaves the line receivable by a correct scan
    Given the destination Site "SITE-DEST" scans 5 units of SKU "SKU-R1" for transfer "tr-WRONG" line "tl-1"
    When the destination Site "SITE-DEST" scans 5 units of SKU "SKU-R1" for transfer "tr-1" line "tl-1"
    Then the response status is 201
    And the transfer receipt is STAGED with expected 5, received 5 and variance 0

  @bdd
  Scenario Outline: A malformed scan is rejected before it is looked up
    When a POST is sent to "/transfers/tl-1/receipt" with the body <body>
    Then the response status is 400
    And exactly 0 "TransferReceiptStaged" domain events are published

    Examples:
      | body                                                                         |
      | {"destinationSiteId":"SITE-DEST","sku":"SKU-R1","receivedQuantity":5}       |
      | {"transferId":"","destinationSiteId":"SITE-DEST","sku":"SKU-R1","receivedQuantity":5} |
      | {"transferId":"tr-1","destinationSiteId":"SITE-DEST","receivedQuantity":5}  |
      | {"transferId":"tr-1","destinationSiteId":"SITE-DEST","sku":"","receivedQuantity":5} |
      | {"transferId":"tr-1","destinationSiteId":"SITE-DEST","sku":                  |

  @bdd
  Scenario: A malformed scan problem names the missing transfer id
    When a POST is sent to "/transfers/tl-1/receipt" with the body {"destinationSiteId":"SITE-DEST","sku":"SKU-R1","receivedQuantity":5}
    Then the problem detail type is "transfer-id-required"

  # Defects found by this suite (apis/openapi.yaml 400: "destinationSiteId
  # missing or empty, receivedQuantity not positive"). Excluded from the run
  # until fixed; they describe the documented behaviour.
  @bdd @known-bug
  Scenario Outline: A scan with an empty destination site or a non-positive quantity is a bad request
    When a POST is sent to "/transfers/tl-1/receipt" with the body <body>
    Then the response status is 400
    And exactly 0 "TransferReceiptStaged" domain events are published

    Examples:
      | body                                                                                         |
      | {"transferId":"tr-1","sku":"SKU-R1","receivedQuantity":5}                                    |
      | {"transferId":"tr-1","destinationSiteId":"","sku":"SKU-R1","receivedQuantity":5}             |
      | {"transferId":"tr-1","destinationSiteId":"SITE-DEST","sku":"SKU-R1","receivedQuantity":0}    |
      | {"transferId":"tr-1","destinationSiteId":"SITE-DEST","sku":"SKU-R1","receivedQuantity":-1}   |
