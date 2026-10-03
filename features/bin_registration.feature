Feature: Bin registration
  Inventory control registers a Bin (a coded slot) and its capacity over the
  REST API — no more seeding bins straight into Postgres. Registration is
  declarative and idempotent: PUT the desired capacity and the Bin converges
  to it. A Bin can never be resized below what it already holds, because the
  stock in it is physically there (ADR 0025).

  Background:
    Given an empty warehouse

  @bdd
  Scenario: Registering a new bin creates it empty
    When I register Bin "R-1-1" with capacity 12
    Then the response status is 201
    And the Bin response reports capacity 12, occupied 0 and available 12
    When I look up Bin "R-1-1"
    Then the response status is 200
    And the Bin response reports capacity 12, occupied 0 and available 12

  @bdd
  Scenario: A registered bin can be stowed into
    Given I register Bin "R-1-2" with capacity 5
    And 5 units of SKU "SKU-B" have been Received
    When I Stow 5 units of SKU "SKU-B" into Bin "R-1-2"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-B" is 5
    When I look up Bin "R-1-2"
    Then the Bin response reports capacity 5, occupied 5 and available 0

  @bdd
  Scenario: Re-registering with the same capacity is a no-op
    Given a Bin "R-1-3" with capacity 10
    And 4 units of SKU "SKU-B" are Stowed into Bin "R-1-3"
    When I register Bin "R-1-3" with capacity 10
    Then the response status is 200
    And the Bin response reports capacity 10, occupied 4 and available 6

  @bdd
  Scenario: Re-registering with a capacity at or above occupancy resizes the bin
    Given a Bin "R-1-4" with capacity 10
    And 4 units of SKU "SKU-B" are Stowed into Bin "R-1-4"
    When I register Bin "R-1-4" with capacity 4
    Then the response status is 200
    And the Bin response reports capacity 4, occupied 4 and available 0

  @bdd
  Scenario: Shrinking a bin below its occupancy is rejected
    Given a Bin "R-1-5" with capacity 10
    And 4 units of SKU "SKU-B" are Stowed into Bin "R-1-5"
    When I register Bin "R-1-5" with capacity 3
    Then the response status is 409
    And the problem detail type is "capacity-below-occupancy"
    When I look up Bin "R-1-5"
    Then the Bin response reports capacity 10, occupied 4 and available 6

  @bdd
  Scenario: A non-positive capacity is rejected
    When I register Bin "R-1-6" with capacity 0
    Then the response status is 422
    And the problem detail type is "invalid-bin-capacity"
    When I look up Bin "R-1-6"
    Then the response status is 404
    And the problem detail type is "bin-not-found"

  @bdd
  Scenario: A reservation tells the picker which bin to go to
    Given I register Bin "R-2-1" with capacity 10
    And 6 units of SKU "SKU-P" are Stowed into Bin "R-2-1"
    When I Reserve 4 units of SKU "SKU-P" for demand "ORDER-PICK-1"
    Then the response status is 201
    And every Reservation allocation is picked from Bin "R-2-1"
    When I look up the Reservations for demand "ORDER-PICK-1"
    Then the response status is 200
    And every allocation in the Reservations list is picked from Bin "R-2-1"
