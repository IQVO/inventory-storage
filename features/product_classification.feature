# Derived from:
#   - docs/docs/adr/0033-product-master-owns-classification.md (product-master
#     ADR 0003 stage C): product-master owns product classification;
#     PUT /products/{sku}/classification returns 410 classification-moved;
#     this service keeps a version-guarded local copy fed by product-master's
#     ProductClassified, and the write path raises no ProductClassified.
#   - apis/openapi.yaml — GET /products/{sku}/classification
#     (getProductClassification, deprecated): served from the local copy;
#     "an unclassified SKU is a 404, not a zero-value response".
#   - .claude/rules/domain-model.md — "ApplyProductClassification ... applies
#     only when version > stored version, raises no domain event".

Feature: Product classification (local copy of product-master)
  A ProductClassification is SKU-level master data describing how an item
  must be handled. product-master owns it; this service keeps a local copy,
  fed by product-master's events, that stow-time placement reads. An
  unclassified SKU carries no constraints at all.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: Classifying through this service is gone and points at product-master
    When I Classify SKU "SKU-P" with handling tags "Fragile"
    Then the response status is 410
    And the problem detail type is "classification-moved"
    And the domain event "ProductClassified" was not published
    When I request the classification for SKU "SKU-P"
    Then the response status is 404

  @bdd
  Scenario: A product-master classification lands in the local copy
    Given product-master has classified SKU "SKU-P" with handling tags "Fragile" at version 1
    When I request the classification for SKU "SKU-P"
    Then the response status is 200
    And the ProductClassification response reports SKU "SKU-P" with handling tags "Fragile"
    And the domain event "ProductClassified" was not published

  @bdd
  Scenario: An out-of-order older version does not overwrite a newer one
    Given product-master has classified SKU "SKU-P" with handling tags "Hazmat" at version 3
    And product-master has classified SKU "SKU-P" with handling tags "Fragile" at version 2
    When I request the classification for SKU "SKU-P"
    Then the response status is 200
    And the ProductClassification response reports SKU "SKU-P" with handling tags "Hazmat"

  @bdd
  Scenario: An unclassified SKU has no classification to read
    When I request the classification for SKU "SKU-NONE"
    Then the response status is 404
    And the problem detail type is "product-classification-not-found"
