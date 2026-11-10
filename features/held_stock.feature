# Derived from:
#   - docs/docs/adr/0052-held-stock-and-disposition.md — a Damaged line of
#     inbound-receiving's ReceiptLineReceived is booked as HELD stock instead
#     of being dropped; a held unit has no usable quantity and can never be
#     reserved; a disposition (RELEASE, RETURN_TO_VENDOR, SCRAP, REWORK)
#     settles it.
#   - The event arrives from Kafka, so it is seeded through
#     BookInboundReceiptLine, the use case the consumer calls; assertions go
#     through REST and the published domain events.

Feature: Held stock and disposition
  Damaged receipt lines are held out of usable stock, visible and decided
  upon, instead of vanishing from the inventory view.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: A damaged receipt line is held, not usable, then returned to vendor
    When inbound-receiving reports line "RL-1" of 4 units of SKU "SKU-H" in Damaged condition
    Then the domain event "StockHeld" was published exactly 1 time
    And the Usable inventory for SKU "SKU-H" is 0
    And the held stock of SKU "SKU-H" is 1 unit of 4 eaches for reason "DAMAGED"
    When I Reserve 1 unit of SKU "SKU-H" for demand "order-held" on line 1
    Then the response status is 409
    When I decide RETURN_TO_VENDOR for the held unit of SKU "SKU-H"
    Then the response status is 200
    And the domain event "StockDispositioned" was published exactly 1 time
    And the held stock of SKU "SKU-H" is 0 units of 0 eaches for reason "DAMAGED"
    And the Usable inventory for SKU "SKU-H" is 0

  @bdd
  Scenario: A redelivered damaged line is held once
    When inbound-receiving reports line "RL-1" of 4 units of SKU "SKU-H" in Damaged condition
    And inbound-receiving reports line "RL-1" of 4 units of SKU "SKU-H" in Damaged condition
    Then the domain event "StockHeld" was published exactly 1 time
    And the held stock of SKU "SKU-H" is 1 unit of 4 eaches for reason "DAMAGED"

  @bdd
  Scenario: A released unit re-enters normal stock, staged until it is stowed
    When inbound-receiving reports line "RL-1" of 4 units of SKU "SKU-H" in Damaged condition
    And I decide RELEASE for the held unit of SKU "SKU-H"
    Then the response status is 200
    And the domain event "StockReleased" was published exactly 1 time
    And the domain event "StockReceived" was published exactly 1 time
    And the held stock of SKU "SKU-H" is 0 units of 0 eaches for reason "DAMAGED"
    And the Usable inventory for SKU "SKU-H" is 0

  @bdd
  Scenario: A rework keeps the unit held under a new reason and a settled unit takes no second decision
    When inbound-receiving reports line "RL-1" of 4 units of SKU "SKU-H" in Damaged condition
    And I decide REWORK for the held unit of SKU "SKU-H"
    Then the response status is 200
    And the held stock of SKU "SKU-H" is 1 unit of 4 eaches for reason "OTHER"
    When I decide SCRAP for the held unit of SKU "SKU-H"
    Then the response status is 200
    When I decide RELEASE for the settled held unit of SKU "SKU-H"
    Then the response status is 409
