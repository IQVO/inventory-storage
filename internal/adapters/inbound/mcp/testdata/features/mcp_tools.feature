# MCP behavioral evals for the inventory-storage tool surface.
#
# Each scenario drives tools/call over the REAL Streamable HTTP handler
# with a connected SDK client — exactly the call a model host makes — and
# pins the structured result and the state side effects. Arguments are
# deliberately model-realistic: extra keys, wrong types, unknown ids.
#
# Derived from the tool contracts documented in:
#   - internal/adapters/inbound/mcp/tools.go (tool descriptions and
#     semantics: usable = on-hand minus reservations and held/unlocated)
#   - docs/docs/mcp/governance-charter.md (§2 tool curation: intent-level
#     tools; §4 write tools must be safe-by-construction)
#   - apis/openapi.yaml GET /stock/usable ("Usable quantity for the SKU
#     (0 if the SKU is unknown)") — the same read model the tool serves.

Feature: MCP tool behavioral evals
  The inventory-storage MCP tools expose this bounded context to AI
  agents: usable availability by SKU, bin occupancy diagnostics, and a
  revocable-reservation write. An agent relying on them must get the same
  semantics the REST API guarantees, through the schema-decoded argument
  path a model host actually uses.

  Background:
    Given the MCP server is running with the canonical eval state (10 units of SKU-A at BIN-1, 4 reserved)

  Scenario: Usable availability reflects active reservations
    When I call the tool "check_availability" with argument "sku" = "SKU-A"
    Then the tool call succeeds
    And the structured result field "sku" is "SKU-A"
    And the structured result field "usable" is 6

  Scenario: An unknown SKU reports zero usable, not an error
    When I call the tool "check_availability" with argument "sku" = "SKU-NEVER-HEARD-OF"
    Then the tool call succeeds
    And the structured result field "usable" is 0

  Scenario: Model chatter in the arguments is rejected, not ignored
    The typed tool schemas are strict (additionalProperties: false, the
    SDK default): a host forwarding stray model-generated keys gets a
    clean schema validation error rather than a silent ignore.
    When I call the tool "check_availability" with arguments
      | sku           | SKU-A            |
      | model_chatter | maybe low?       |
      | step          | 2                |
    Then the tool call reports a problem mentioning "model_chatter"

  Scenario: A wrong-typed argument is rejected without coercion
    When I call the tool "check_availability" with argument "sku" = 42
    Then the tool call does not succeed silently

  Scenario: Bin occupancy breaks the bin down per StockUnit
    When I call the tool "get_bin_occupancy" with argument "binId" = "BIN-1"
    Then the tool call succeeds
    And the structured result field "binId" is "BIN-1"
    And the structured result field "onHand" is 10
    And the structured result field "reserved" is 4
    And the structured result field "usable" is 6

  Scenario: An unknown bin reports an empty occupancy, not an error
    The read port cannot distinguish an unknown bin from an empty one —
    both hold nothing. Pinned here so the ambiguity is a visible,
    documented contract rather than a surprise.
    When I call the tool "get_bin_occupancy" with argument "binId" = "BIN-404"
    Then the tool call succeeds
    And the structured result field "binId" is "BIN-404"
    And the structured result field "onHand" is 0
    And the structured result field "unitCount" is 0

  Scenario: Revoking a reservation returns its quantity to usable
    When I call the tool "revoke_reservation" with the seeded reservation id
    Then the tool call succeeds
    And the structured result field "revoked" is true
    And the domain event "ReservationRevoked" was published
    When I call the tool "check_availability" with argument "sku" = "SKU-A"
    Then the tool call succeeds
    And the structured result field "usable" is 10

  Scenario: Revoking an unknown reservation is a clean tool error
    When I call the tool "revoke_reservation" with argument "reservationId" = "res-does-not-exist"
    Then the tool call reports a problem mentioning "res-does-not-exist"

  Scenario: An empty reservation id is a clean tool error
    When I call the tool "revoke_reservation" with argument "reservationId" = ""
    Then the tool call reports a problem mentioning "reservationId"
