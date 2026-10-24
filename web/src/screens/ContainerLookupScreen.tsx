import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { Card, StatusPill } from "@warehouse/ui-kit";
import { ApiError, LPN_PATTERN, getContainer } from "../api";
import { describeLookupError, invalidLpnCopy, type ErrorCopy } from "../containerErrors";
import type { Container, ContainerState } from "../types";

/** Outcome of the request for one LPN. Idle / invalid / loading are derived
 *  during render from the URL and this state, never set inside the effect. */
type Outcome =
  | { lpn: string; phase: "found"; container: Container }
  | { lpn: string; phase: "error"; copy: ErrorCopy };

const STATE_TONE = { SEALED: "success", OPEN: "progress", EMPTY: "neutral" } as const;

const STATE_NOTE: Record<ContainerState, string> = {
  SEALED: "Unopened case, stored as one unit.",
  OPEN: "Being broken into loose eaches.",
  EMPTY: "Broken: its eaches are now loose stock and the case holds nothing.",
};

const inputStyle = {
  flex: 1,
  maxWidth: 360,
  padding: "10px 12px",
  borderRadius: "var(--wh-radius-md)",
  border: "1px solid var(--wh-color-border)",
  background: "var(--wh-color-bg-sunken)",
  color: "var(--wh-color-text)",
  fontFamily: "var(--wh-font-mono)",
  fontSize: "var(--wh-font-size-sm)",
} as const;

/**
 * Container lookup (ADR 0045): scan or type an LPN / SSCC and read the case --
 * state, SKU, case GTIN, units per pack, quantity in eaches, bin, owner and
 * the receipt line it came from. Read-only: breaking and reserving cases
 * belong to the floor client. The searched LPN lives in `?lpn=` so a result is
 * a shareable link.
 */
export function ContainerLookupScreen() {
  const [params, setParams] = useSearchParams();
  const lpn = params.get("lpn")?.trim() ?? "";
  const [query, setQuery] = useState(lpn);
  const [outcome, setOutcome] = useState<Outcome | null>(null);
  const valid = lpn !== "" && LPN_PATTERN.test(lpn);

  useEffect(() => {
    if (!valid) return;
    let cancelled = false;
    getContainer(lpn).then(
      (container) => {
        if (!cancelled) setOutcome({ lpn, phase: "found", container });
      },
      (err: unknown) => {
        if (cancelled) return;
        const apiErr = err instanceof ApiError ? err : new ApiError(0, null, String(err));
        setOutcome({ lpn, phase: "error", copy: describeLookupError(apiErr, lpn) });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [lpn, valid]);

  // The outcome only counts while it answers the LPN currently in the URL.
  const current = outcome && outcome.lpn === lpn ? outcome : null;
  const loading = valid && current === null;

  function onSubmit(e: FormEvent) {
    e.preventDefault();
    const trimmed = query.trim();
    if (trimmed) setParams({ lpn: trimmed });
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <div>
        <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>Container lookup</h1>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>
          Scan or type a carton SSCC / LPN to see its state, contents and location.
        </p>
      </div>

      <form
        onSubmit={onSubmit}
        aria-label="Container search"
        style={{ display: "flex", gap: "var(--wh-space-2)" }}
      >
        <input
          aria-label="LPN or SSCC"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="LPN or SSCC"
          autoFocus
          autoComplete="off"
          style={inputStyle}
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
          Look up
        </button>
      </form>

      {loading && (
        <div role="status" style={{ color: "var(--wh-color-text-muted)" }}>
          Looking up {lpn}…
        </div>
      )}

      {lpn !== "" && !valid && <ErrorCard copy={invalidLpnCopy(lpn)} />}
      {current?.phase === "error" && <ErrorCard copy={current.copy} />}
      {current?.phase === "found" && <ContainerCard container={current.container} />}
    </div>
  );
}

function ErrorCard({ copy }: { copy: ErrorCopy }) {
  return (
    <Card>
      <div role="alert" style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        <strong style={{ color: "var(--wh-color-status-danger)" }}>{copy.what}</strong>
        <span>{copy.todo}</span>
      </div>
    </Card>
  );
}

function ContainerCard({ container: c }: { container: Container }) {
  const owner = c.ownerRef ? c.ownerRef : "Fleet-owned";
  const fields: Array<[string, ReactNode]> = [
    ["SKU", c.sku],
    ["Case GTIN", c.gtin],
    ["Units per pack", c.unitsPerPack],
    ["Quantity", `${c.qtyEaches} eaches`],
    ["Bin", c.binId ?? (c.state === "EMPTY" ? "None (case is empty)" : "Not stowed yet")],
    ["Site", c.siteId ?? "—"],
    ["Owner / seller", owner],
    ["Lot", c.lot ?? "—"],
    ["Expiry", c.expiry ?? "—"],
    [
      "Receipt",
      <a
        key="receipt"
        href={`/inbound-receiving/receipts/${encodeURIComponent(c.receiptId)}`}
      >
        {c.receiptId} · line {c.lineNo}
      </a>,
    ],
  ];

  return (
    <Card title={c.lpn} actions={<StatusPill status={c.state} tone={STATE_TONE[c.state]} />}>
      <p style={{ marginTop: 0, color: "var(--wh-color-text-muted)" }}>{STATE_NOTE[c.state]}</p>
      <dl
        style={{
          display: "grid",
          gridTemplateColumns: "max-content 1fr",
          gap: "var(--wh-space-2) var(--wh-space-5)",
          margin: 0,
          fontSize: "var(--wh-font-size-sm)",
        }}
      >
        {fields.map(([label, value]) => (
          <div key={label} style={{ display: "contents" }}>
            <dt style={{ color: "var(--wh-color-text-muted)" }}>{label}</dt>
            <dd style={{ margin: 0, fontFamily: "var(--wh-font-mono)" }}>{value}</dd>
          </div>
        ))}
      </dl>
    </Card>
  );
}
