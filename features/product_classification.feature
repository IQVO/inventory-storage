# Derived from:
#   - docs/docs/adr/0034-product-master-owns-classification.md (product-master
#     ADR 0003): product-master owns product classification; this service
#     keeps a version-guarded local copy fed by product-master's
#     ProductClassified, and the write path raises no ProductClassified.
#     Stage E (executed 2026-10-17) retired PUT and GET
#     /products/{sku}/classification — both routes are gone.
#   - .claude/rules/domain-model.md — "ApplyProductClassification ... applies
#     only when version > stored version, raises no domain event".
# The local copy's stow-time effect is covered by stow_placement.feature;
# the consumer's version guard by product_master_consumer_test.go.

Feature: Product classification (retired surface; local copy of product-master)
  A ProductClassification is SKU-level master data describing how an item
  must be handled. product-master owns it; this service keeps a local copy,
  fed by product-master's events, that stow-time placement reads. The
  retired REST surface answers 404. An unclassified SKU carries no
  constraints at all.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: The retired classification surface is gone
    When I Classify SKU "SKU-P" with handling tags "Fragile"
    Then the response status is 404
    When I request the classification for SKU "SKU-P"
    Then the response status is 404
    And the domain event "ProductClassified" was not published
