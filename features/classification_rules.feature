# Documentation-derived rules for ApplyProductClassification (use case #8) and
# the ProductClassification aggregate canvas (invariants P1-P8, "re-classification
# replaces", version guard, idempotent claim).
#
# Derived from:
#   - use-cases.md #8: "the upsert applies only when the message's version is
#     greater than the stored one (legacy rows are version 0), so redeliveries
#     and out-of-order messages are harmless"; malformed messages (broken
#     invariants, no id, version below 1) are ErrMalformedProductClassification.
#   - aggregate-design-canvas.md "ProductClassification" P1-P8 and the state
#     machine Unclassified -> Classified -> Classified (newer version replaces).
# The message arrives from Kafka (warehouse.product-master.events), so it is
# driven through the use case the consumer calls; the local copy is asserted
# directly and its stow-time effect through POST /stowages.

Feature: Local copy of product-master's classification
  product-master owns the classification. This service keeps a version-guarded
  local copy that stow-time placement reads: a stale or repeated message never
  rewinds it, and a message that breaks an invariant never enters it.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: A newer version replaces the copy while an older, an equal or a redelivered message never does
    When product-master sends classification event "pm-1" for SKU "SKU-CL" with tags "Fragile", temperature class "", DOT class 0 and version 1
    Then the classification update is APPLIED
    And the local classification of SKU "SKU-CL" is "Fragile" at version 1
    When product-master sends classification event "pm-2" for SKU "SKU-CL" with tags "Hazmat", temperature class "", DOT class 3 and version 2
    Then the classification update is APPLIED
    And the local classification of SKU "SKU-CL" is "Hazmat" at version 2
    When product-master sends classification event "pm-3" for SKU "SKU-CL" with tags "Fragile", temperature class "", DOT class 0 and version 1
    Then the classification update is STALE
    When product-master sends classification event "pm-4" for SKU "SKU-CL" with tags "HighValue", temperature class "", DOT class 0 and version 2
    Then the classification update is STALE
    When product-master sends classification event "pm-2" for SKU "SKU-CL" with tags "Oversized", temperature class "", DOT class 0 and version 5
    Then the classification update is DUPLICATE
    And the local classification of SKU "SKU-CL" is "Hazmat" at version 2
    And no domain events were published

  @bdd
  Scenario: An out-of-order older message cannot lift a hazmat restriction
    Given a Bin "OO-1" with capacity 10
    And Bin "OO-1" sits in a non-hazmat "Ambient" zone
    And product-master sends classification event "pm-b" for SKU "SKU-OO" with tags "Hazmat", temperature class "", DOT class 3 and version 2
    And product-master sends classification event "pm-a" for SKU "SKU-OO" with tags "Fragile", temperature class "", DOT class 0 and version 1
    Then the classification update is STALE
    When I Stow 2 units of SKU "SKU-OO" into Bin "OO-1"
    Then the response status is 409
    And the problem detail type is "hazmat-zone-required"

  @bdd
  Scenario Outline: A message that breaks a classification invariant is refused as malformed and enters no copy
    When product-master sends classification event "<event>" for SKU "<sku>" with tags "<tags>", temperature class "<temperature>", DOT class <dot> and version <version>
    Then the classification update is refused as malformed
    And SKU "SKU-CL" has no local classification

    Examples:
      | event | sku    | tags                 | temperature | dot | version | broken rule                                   |
      |       | SKU-CL | Fragile              |             | 0   | 1       | no CloudEvents id                             |
      | pm-1  |        | Fragile              |             | 0   | 1       | empty SKU                                     |
      | pm-1  | SKU-CL |                      |             | 0   | 1       | P1 at least one handling tag                  |
      | pm-1  | SKU-CL | Perishable           |             | 0   | 1       | P2 unknown handling tag                       |
      | pm-1  | SKU-CL | Fragile, Fragile     |             | 0   | 1       | P3 tags form a set                            |
      | pm-1  | SKU-CL | TemperatureSensitive |             | 0   | 1       | P4 temperature class required                 |
      | pm-1  | SKU-CL | TemperatureSensitive | Tepid       | 0   | 1       | P4 unknown temperature class                  |
      | pm-1  | SKU-CL | Fragile              | Chilled     | 0   | 1       | P5 class without TemperatureSensitive         |
      | pm-1  | SKU-CL | Fragile              |             | 3   | 1       | P6 DOT class without Hazmat                   |
      | pm-1  | SKU-CL | Hazmat               |             | 10  | 1       | P7 DOT class above 9                          |
      | pm-1  | SKU-CL | Fragile              |             | 0   | 0       | version below 1                               |

  @bdd
  Scenario Outline: Hazmat needs no DOT class and accepts every class from 1 to 9
    When product-master sends classification event "pm-1" for SKU "SKU-CL" with tags "Hazmat", temperature class "", DOT class <dot> and version 1
    Then the classification update is APPLIED
    And the local classification of SKU "SKU-CL" is "Hazmat" at version 1

    Examples:
      | dot |
      | 0   |
      | 1   |
      | 9   |
