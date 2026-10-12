# Derived from:
#   - docs/docs/adr/0036-per-line-confirm-pick.md — a reservation optionally
#     names its order line (lineNo, 1..2147483647); the replay guard keys on
#     (demandRef, sku, quantity, lineNo), so a retry of one line returns the
#     existing reservation while another line of the same order is a new one.
#   - apis/openapi.yaml — POST /reservations (400 invalid-line-no), DELETE
#     /reservations/{id} and POST /reservations/{id}/confirm-pick (404
#     reservation-not-found); a reservation draws across StockUnits, and a
#     confirmed pick releases every bin it drew from.

Feature: Reservation order lines and allocation
  One demand (an order) holds several reservations at once — one per line —
  and a single reservation may draw from several bins.

  Background:
    Given an empty warehouse
    And a Bin "RL-1" with capacity 30
    And 10 units of SKU "SKU-RA" are Stowed into Bin "RL-1"
    And 10 units of SKU "SKU-RB" are Stowed into Bin "RL-1"

  @bdd
  Scenario: A line number outside the valid range is rejected
    When I Reserve 2 units of SKU "SKU-RA" for demand "ORDER-9" on line 0
    Then the response status is 400
    And the problem detail type is "invalid-line-no"
    And the Usable inventory for SKU "SKU-RA" is 10

  @bdd
  Scenario: Each line of an order is its own reservation and only an identical line is a retry
    Given a Reservation of 2 units of SKU "SKU-RA" for demand "ORDER-9" on line 1
    When I Reserve 3 units of SKU "SKU-RB" for demand "ORDER-9" on line 2
    Then the response status is 201
    And the Reservation response reports line 2
    When I Reserve 2 units of SKU "SKU-RA" for demand "ORDER-9" on line 1
    Then the Usable inventory for SKU "SKU-RA" is 8
    When I Reserve 2 units of SKU "SKU-RA" for demand "ORDER-9" on line 3
    Then the Usable inventory for SKU "SKU-RA" is 6
    When I look up the Reservations for demand "ORDER-9"
    Then the Reservations list contains 3 entries

  @bdd
  Scenario: A reservation drawn from two bins releases both on pick
    Given a Bin "RL-2" with capacity 10
    And a Bin "RL-3" with capacity 10
    And 4 units of SKU "SKU-RM" are Stowed into Bin "RL-2"
    And 4 units of SKU "SKU-RM" are Stowed into Bin "RL-3"
    When I Reserve 8 units of SKU "SKU-RM" for demand "ORDER-M"
    Then the response status is 201
    And the Reservation response has 2 allocations
    When I Confirm the pick for the Reservation
    Then the response status is 204
    When I look up Bin "RL-2"
    Then the Bin response reports capacity 10, occupied 0 and available 10
    When I look up Bin "RL-3"
    Then the Bin response reports capacity 10, occupied 0 and available 10
    And the Usable inventory for SKU "SKU-RM" is 0

  @bdd
  Scenario: A reservation that does not exist can be neither revoked nor confirmed
    When I Revoke the Reservation with id "res-nope"
    Then the response status is 404
    And the problem detail type is "reservation-not-found"
    When I Confirm the pick for the Reservation with id "res-nope"
    Then the response status is 404
    And the problem detail type is "reservation-not-found"
