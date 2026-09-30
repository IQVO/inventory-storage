# Derived from:
#   - apis/openapi.yaml — POST /stock/receive (receiveStock): "Stages a
#     quantity of a SKU as received into the building (publishes
#     StockReceived) without assigning it to a bin yet. This does NOT
#     create a StockUnit aggregate ... The durable, addressable record only
#     starts at StowStock — that is why this operation returns 202 Accepted
#     rather than 201 Created."
#   - .claude/rules/domain-model.md — "Use cases": "ReceiveStock(sku, qty)
#     -> staged stock awaiting stow"; "Design notes": "ReceiveStock does
#     not create a StockUnit. A StockUnit requires both a SKU and a Bin
#     (item-scan + location-scan) by construction."

Feature: Receive
  Receiving acknowledges inbound goods into the building (item-scan only)
  and stages them awaiting Stow. No StockUnit exists yet — without a
  location-scan there is no bin, so there is nothing usable to promise.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: Receiving stock stages it without creating a StockUnit
    When I Receive 5 units of SKU "SKU-RC"
    Then the response status is 202
    And the Staged receipt response reports SKU "SKU-RC" with quantity 5
    And the domain event "StockReceived" was published
    And the Usable inventory for SKU "SKU-RC" is 0
