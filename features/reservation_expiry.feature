# Derived from:
#   - docs/docs/business-context/revocable-reservations.md — "Lazy expiry,
#     not a sweeper": ReservationExpired "is genuinely raised — but only
#     when something reads the reservation (listing it by demand ref,
#     revoking it, confirming its pick, or retrying ReserveStock against
#     the same demand ref) after its window has closed. There is still no
#     background timer."
#   - docs/docs/ddd/domain-events.md#lazy-expiry-no-sweeper-resolved-at-the-next-read
#     — the read-time resolution contract and its exactly-once guarantee.
#   - apis/asyncapi.yaml — ReservationExpired: "Raised ... the first time
#     the reservation is read (GetReservationsByDemandRef,
#     RevokeReservation, ConfirmPick, or ReserveStock's own idempotency
#     lookup) after its 30-minute timeout"; the expiry releases the
#     reserved quantity back to usable.
#   - apis/openapi.yaml — POST /reservations (reserveStock): "Idempotent
#     (best-effort) by demandRef ... A demandRef whose only reservation
#     is revoked/confirmed/expired is still treated as a genuine new
#     attempt" — so a retry after expiry creates a fresh reservation.
#   - apis/openapi.yaml — Reservation schema, expiresAt: "Timestamp this
#     reservation auto-expires if never confirmed or revoked" (created
#     plus the 30-minute default timeout, DefaultReservationTimeout).

Feature: Reservation expiry
  A Reservation expires automatically after its timeout if never confirmed
  or revoked. There is no background sweeper: expiry is resolved lazily
  the next time anything reads the reservation. The read releases the
  reserved quantity back to usable, flips the reservation to EXPIRED, and
  raises ReservationExpired — so a late confirm or revoke is rejected as
  already resolved, and a retry for the same demand is a genuine new
  attempt.

  Background:
    Given an empty warehouse
    And a Bin "E-1-1" with capacity 20
    And 10 units of SKU "SKU-E" are Stowed into Bin "E-1-1"

  @bdd
  Scenario: A timed-out reservation is expired at the next read
    Given a Reservation of 4 units of SKU "SKU-E" for demand "ORDER-READ"
    And time advances by 31 minutes
    And the Usable inventory for SKU "SKU-E" is 6
    When I look up the Reservations for demand "ORDER-READ"
    Then the response status is 200
    And the Reservations list contains 1 entry
    And the Reservations list contains a Reservation with status "EXPIRED"
    And the Usable inventory for SKU "SKU-E" is 10
    And the domain event "ReservationExpired" was published

  @bdd
  Scenario: Confirming a timed-out reservation is rejected as already resolved
    Given a Reservation of 4 units of SKU "SKU-E" for demand "ORDER-EXP"
    And time advances by 31 minutes
    When I Confirm the pick for the Reservation
    Then the response status is 409
    And the problem detail type is "reservation-already-resolved"
    And the Usable inventory for SKU "SKU-E" is 10
    And the domain event "ReservationExpired" was published

  @bdd
  Scenario: A timed-out reservation can no longer be revoked
    Given a Reservation of 4 units of SKU "SKU-E" for demand "ORDER-LATE"
    And time advances by 31 minutes
    When I Revoke the Reservation
    Then the response status is 409
    And the problem detail type is "reservation-already-resolved"
    And the Usable inventory for SKU "SKU-E" is 10
    And the domain event "ReservationRevoked" was not published

  @bdd
  Scenario: Retrying a reservation after a timeout is a genuine new attempt
    Given a Reservation of 4 units of SKU "SKU-E" for demand "ORDER-RETRY"
    And time advances by 31 minutes
    When I Reserve 4 units of SKU "SKU-E" for demand "ORDER-RETRY"
    Then the response status is 201
    And the Usable inventory for SKU "SKU-E" is 6
    When I look up the Reservations for demand "ORDER-RETRY"
    Then the response status is 200
    And the Reservations list contains 2 entries
    And the Reservations list contains a Reservation with status "EXPIRED"
    And the Reservations list contains a Reservation with status "ACTIVE"
