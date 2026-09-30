# Derived from:
#   - apis/openapi.yaml — PUT /products/{sku}/classification
#     (classifyProduct): "Registers or replaces the handling
#     classification for a SKU — SKU-level master data this service owns as
#     source of truth (see ADR 0009)"; 201 for a new classification, 200
#     when replacing; 400 temperature-class-required "when (and only when)
#     handlingTags includes TemperatureSensitive, temperatureClass is
#     required".
#   - apis/openapi.yaml — GET /products/{sku}/classification
#     (getProductClassification): "Unlike GetUsableInventory, an
#     unclassified SKU is a 404, not a zero-value response — there is no
#     meaningful 'empty' classification to return."
#   - .claude/rules/domain-model.md — "Aggregates & invariants":
#     "ProductClassification: TemperatureSensitive requires a non-empty,
#     valid TemperatureClass"; "Domain events": ProductClassified;
#     "Use cases": "ClassifyProduct(sku, handlingTags, temperatureClass?,
#     dotHazardClass?) -> registers/replaces a SKU's
#     ProductClassification".

Feature: Product classification
  A ProductClassification is SKU-level master data describing how an item
  must be handled, independent of any Bin or StockUnit. This service is
  the source of truth: classifying registers or replaces it, and an
  unclassified SKU carries no constraints at all.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: Classifying a SKU registers its handling classification
    When I Classify SKU "SKU-P" with handling tags "Fragile"
    Then the response status is 201
    And the ProductClassification response reports SKU "SKU-P" with handling tags "Fragile"
    And the domain event "ProductClassified" was published
    When I request the classification for SKU "SKU-P"
    Then the response status is 200
    And the ProductClassification response reports SKU "SKU-P" with handling tags "Fragile"

  @bdd
  Scenario: A temperature-sensitive classification requires a temperature class
    When I Classify SKU "SKU-TS" as TemperatureSensitive without a temperature class
    Then the response status is 400
    And the problem detail type is "temperature-class-required"

  @bdd
  Scenario: An unclassified SKU has no classification to read
    When I request the classification for SKU "SKU-NONE"
    Then the response status is 404
    And the problem detail type is "product-classification-not-found"
