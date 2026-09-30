# Derived from:
#   - apis/openapi.yaml — GET /reservations (getReservationsByDemandRef):
#     "Returns every Reservation ever created against a caller-supplied
#     demandRef ... an unknown demandRef returns 200 with an empty array,
#     not 404"; the demandRef query parameter is required (400
#     missing-demand-ref, "there is no list-all semantics for this
#     endpoint"); the response example shows one REVOKED and one ACTIVE
#     reservation for the same demandRef (a revoke followed by a retry).
#   - apis/openapi.yaml — POST /reservations (reserveStock): "Idempotent
#     (best-effort) by demandRef: retrying this call after a dropped
#     response returns the existing ACTIVE reservation for the same
#     demandRef instead of creating a second one".
#   - .claude/rules/rest-api.md — "GET /reservations?demandRef= is the read
#     side backing the fleet's cross-service Order Lifecycle console
#     screen ... It returns every Reservation ever created against a
#     caller-supplied demandRef (array, since a demandRef can have multiple
#     reservations across its lifetime — a revoke followed by a retry),
#     never 404s on an unknown demandRef (200 + empty array instead)".

Feature: Demand reference read side
  A demandRef identifies the external demand a Reservation satisfies. A
  single demandRef can accumulate several Reservations across its lifetime
  — a revoke followed by a retry — and the read side must show them all,
  without ever inventing a second Reservation for a retry of the same
  demand while one is still active.

  Background:
    Given an empty warehouse
    And a Bin "D-1-1" with capacity 20
    And 10 units of SKU "SKU-D" are Stowed into Bin "D-1-1"

  @bdd
  Scenario: An unknown demandRef returns an empty list, never 404
    When I look up the Reservations for demand "ORDER-UNKNOWN"
    Then the response status is 200
    And the Reservations list contains 0 entries

  @bdd
  Scenario: Retrying a reservation for the same demand returns the existing one
    When I Reserve 4 units of SKU "SKU-D" for demand "ORDER-IDEM"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-D" is 6
    When I Reserve 4 units of SKU "SKU-D" for demand "ORDER-IDEM"
    Then the response status is 201
    And the Reservation response reports quantity 4 for demand "ORDER-IDEM"
    And the Usable inventory for SKU "SKU-D" is 6
    When I look up the Reservations for demand "ORDER-IDEM"
    Then the response status is 200
    And the Reservations list contains 1 entry
    And the Reservations list contains a Reservation with status "ACTIVE"

  @bdd
  Scenario: Every reservation ever created against a demandRef is returned
    Given a Reservation of 4 units of SKU "SKU-D" for demand "ORDER-42"
    When I Revoke the Reservation
    Then the response status is 204
    When I Reserve 4 units of SKU "SKU-D" for demand "ORDER-42"
    Then the response status is 201
    When I look up the Reservations for demand "ORDER-42"
    Then the response status is 200
    And the Reservations list contains 2 entries
    And the Reservations list contains a Reservation with status "REVOKED"
    And the Reservations list contains a Reservation with status "ACTIVE"

  @bdd
  Scenario: Looking up reservations without a demandRef is rejected
    When I look up the Reservations without a demandRef
    Then the response status is 400
    And the problem detail type is "missing-demand-ref"
