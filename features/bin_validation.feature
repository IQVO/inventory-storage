# Derived from:
#   - apis/openapi.yaml — PUT /bins/{binId}: 400 capacity-required when the
#     body omits capacity (an explicit 0 is a different error, 422
#     invalid-bin-capacity); nothing is registered by a rejected call.

Feature: Bin registration validation
  Registration is declarative, so the desired capacity must be stated.

  Background:
    Given an empty warehouse

  @bdd
  Scenario: Registering a bin without stating a capacity is rejected and creates nothing
    When I register Bin "V-1-1" without a capacity
    Then the response status is 400
    And the problem detail type is "capacity-required"
    When I look up Bin "V-1-1"
    Then the response status is 404
    And the problem detail type is "bin-not-found"
