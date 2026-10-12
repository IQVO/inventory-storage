# Derived from:
#   - docs/docs/adr/0037-stock-receipts-from-inbound-receiving.md —
#     inbound-receiving's ReceiptLineReceived books the Good units of a line
#     as staged stock through the same use case as POST /stock/receive;
#     Damaged units are acknowledged but not booked; a redelivered event is
#     applied once.
#   - The event arrives from Kafka, so it is seeded through
#     BookInboundReceiptLine, the use case the consumer calls; assertions go
#     through REST and the published domain events.

Feature: Inbound receipt booking
  Received goods are staged, not usable, until they are stowed. Only Good
  units are booked.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: A Good line is staged once, a Damaged line is not booked, a redelivery changes nothing
    When inbound-receiving reports line "RL-1" of 5 units of SKU "SKU-I" in Good condition
    And inbound-receiving reports line "RL-2" of 2 units of SKU "SKU-I" in Damaged condition
    And inbound-receiving reports line "RL-1" of 5 units of SKU "SKU-I" in Good condition
    Then the domain event "StockReceived" was published exactly 1 time
    And the Usable inventory for SKU "SKU-I" is 0
