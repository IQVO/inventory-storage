import { useState, type FormEvent } from "react";
import { INVENTORY_API_BASE } from "../config";
import type { UsableInventory } from "../types";
import { useReservations } from "../useReservations";
import { Card, StatusPill, DataTable, useFetch } from "@warehouse/ui-kit";

/**
 * Two independent lookups, both scoped to what inventory-storage's REST
 * API actually exposes today (see CLAUDE.md's REST API section):
 *
 *  1. SKU -> usable inventory (GET /inventory/{sku}/usable) -- the
 *     usable-not-total read model that constrains release.
 *  2. demandRef -> reservations (GET /reservations?demandRef=, keyset-paged
 *     {items, nextCursor}; every page is read) -- every Reservation raised
 *     against that demand, however many allocations each ended up split
 *     across.
 *
 * Mirrors OrdersScreen.tsx's search-by-exact-key pattern: there is no
 * list-all/browse endpoint for either resource, so both sections are
 * simple single-field lookups, not filterable tables.
 */
export function InventoryScreen() {
  const [skuQuery, setSkuQuery] = useState("");
  const [sku, setSku] = useState<string | null>(null);

  const [demandRefQuery, setDemandRefQuery] = useState("");
  const [demandRef, setDemandRef] = useState<string | null>(null);

  const usableUrl = sku
    ? `${INVENTORY_API_BASE}/inventory/${encodeURIComponent(sku)}/usable`
    : null;
  const { data: usable, loading: usableLoading, error: usableError } =
    useFetch<UsableInventory>(usableUrl);

  // GET /reservations answers {items, nextCursor}; useReservations reads
  // every page so a demandRef with many reservations is never truncated.
  const {
    data: reservations,
    loading: reservationsLoading,
    error: reservationsError,
  } = useReservations(demandRef);

  function onSkuSubmit(e: FormEvent) {
    e.preventDefault();
    const trimmed = skuQuery.trim();
    if (trimmed) setSku(trimmed);
  }

  function onDemandRefSubmit(e: FormEvent) {
    e.preventDefault();
    const trimmed = demandRefQuery.trim();
    if (trimmed) setDemandRef(trimmed);
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <div>
        <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>Inventory</h1>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>
          inventory-storage · usable-inventory lookup, chaotic stow, revocable reservations
        </p>
      </div>

      <form onSubmit={onSkuSubmit} style={{ display: "flex", gap: "var(--wh-space-2)" }}>
        <input
          value={skuQuery}
          onChange={(e) => setSkuQuery(e.target.value)}
          placeholder="SKU"
          style={{
            flex: 1,
            maxWidth: 360,
            padding: "10px 12px",
            borderRadius: "var(--wh-radius-md)",
            border: "1px solid var(--wh-color-border)",
            background: "var(--wh-color-bg-sunken)",
            color: "var(--wh-color-text)",
            fontFamily: "var(--wh-font-mono)",
            fontSize: "var(--wh-font-size-sm)",
          }}
        />
        <button
          type="submit"
          style={{
            padding: "10px 18px",
            borderRadius: "var(--wh-radius-md)",
            border: "none",
            background: "var(--wh-color-accent)",
            color: "#fff",
            fontWeight: 600,
            fontSize: "var(--wh-font-size-sm)",
            cursor: "pointer",
          }}
        >
          Look up usable
        </button>
      </form>

      {usableError && (
        <Card>
          <div style={{ color: "var(--wh-color-status-danger)" }}>
            {usableError.message}
          </div>
        </Card>
      )}

      {sku && (usableLoading || usable) && (
        <Card title={sku}>
          {usableLoading ? (
            <div
              style={{
                height: 14,
                width: "40%",
                borderRadius: 4,
                background: "var(--wh-color-border-subtle)",
              }}
            />
          ) : (
            <div
              style={{
                display: "flex",
                gap: "var(--wh-space-6)",
                fontSize: "var(--wh-font-size-sm)",
              }}
            >
              <span>
                Usable quantity:{" "}
                <strong style={{ fontFamily: "var(--wh-font-mono)" }}>
                  {usable?.usable}
                </strong>
              </span>
            </div>
          )}
        </Card>
      )}

      <div>
        <h2 style={{ fontSize: "var(--wh-font-size-lg)", margin: 0 }}>Reservations by demand</h2>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4, marginBottom: 0 }}>
          Search every reservation raised against a demandRef.
        </p>
      </div>

      <form onSubmit={onDemandRefSubmit} style={{ display: "flex", gap: "var(--wh-space-2)" }}>
        <input
          value={demandRefQuery}
          onChange={(e) => setDemandRefQuery(e.target.value)}
          placeholder="Demand ref"
          style={{
            flex: 1,
            maxWidth: 360,
            padding: "10px 12px",
            borderRadius: "var(--wh-radius-md)",
            border: "1px solid var(--wh-color-border)",
            background: "var(--wh-color-bg-sunken)",
            color: "var(--wh-color-text)",
            fontFamily: "var(--wh-font-mono)",
            fontSize: "var(--wh-font-size-sm)",
          }}
        />
        <button
          type="submit"
          style={{
            padding: "10px 18px",
            borderRadius: "var(--wh-radius-md)",
            border: "none",
            background: "var(--wh-color-accent)",
            color: "#fff",
            fontWeight: 600,
            fontSize: "var(--wh-font-size-sm)",
            cursor: "pointer",
          }}
        >
          Search
        </button>
      </form>

      {reservationsError && (
        <Card>
          <div style={{ color: "var(--wh-color-status-danger)" }}>
            {reservationsError.message}
          </div>
        </Card>
      )}

      {demandRef && (
        <Card title={demandRef}>
          <DataTable
            rowKey={(r) => r.id}
            rows={reservations ?? []}
            loading={reservationsLoading}
            emptyState={
              <div
                style={{
                  textAlign: "center",
                  color: "var(--wh-color-text-muted)",
                  fontSize: "var(--wh-font-size-sm)",
                }}
              >
                No reservations found for this demand ref.
              </div>
            }
            columns={[
              { key: "id", header: "Reservation", render: (r) => r.id },
              { key: "sku", header: "SKU", render: (r) => r.sku },
              { key: "quantity", header: "Qty", render: (r) => r.quantity, align: "right" },
              {
                key: "status",
                header: "Status",
                render: (r) => <StatusPill status={r.status} size="sm" />,
              },
              {
                key: "allocations",
                header: "Allocations",
                render: (r) => r.allocations.length,
                align: "right",
              },
              { key: "createdAt", header: "Created", render: (r) => r.createdAt },
              { key: "expiresAt", header: "Expires", render: (r) => r.expiresAt },
            ]}
          />
        </Card>
      )}
    </div>
  );
}
