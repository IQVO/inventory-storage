import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { InventoryScreen } from "./InventoryScreen";
import type { Reservation } from "../types";

function reservation(id: string): Reservation {
  return {
    id,
    sku: "SKU-1",
    quantity: 6,
    demandRef: "order-42",
    status: "ACTIVE",
    allocations: [{ stockUnitId: "su-1", quantity: 6 }],
    createdAt: "2026-08-22T14:00:00Z",
    expiresAt: "2026-08-22T14:50:00Z",
  };
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

describe("InventoryScreen reservations by demandRef", () => {
  it("shows the reservations of every page, not just the first", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (url.searchParams.get("cursor") === "next-page") {
        return json({ items: [reservation("res-second-page")] });
      }
      return json({ items: [reservation("res-first-page")], nextCursor: "next-page" });
    });
    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup();
    render(<InventoryScreen />);
    await user.type(screen.getByPlaceholderText("Demand ref"), "order-42");
    await user.click(screen.getByRole("button", { name: "Search" }));

    expect(await screen.findByText("res-first-page")).toBeInTheDocument();
    expect(await screen.findByText("res-second-page")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("shows the empty state when the demandRef has no reservations", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => json({ items: [] })));

    const user = userEvent.setup();
    render(<InventoryScreen />);
    await user.type(screen.getByPlaceholderText("Demand ref"), "nobody");
    await user.click(screen.getByRole("button", { name: "Search" }));

    expect(await screen.findByText("No reservations found for this demand ref.")).toBeInTheDocument();
  });

  it("surfaces the problem detail when the service rejects the lookup", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        json(
          {
            type: "https://errors.inventory-storage.warehouse-systems.dev/missing-demand-ref",
            title: "demandRef query parameter is required",
            detail: "demandRef query parameter must not be empty",
          },
          400,
        ),
      ),
    );

    const user = userEvent.setup();
    render(<InventoryScreen />);
    await user.type(screen.getByPlaceholderText("Demand ref"), "x");
    await user.click(screen.getByRole("button", { name: "Search" }));

    expect(await screen.findByText("demandRef query parameter must not be empty")).toBeInTheDocument();
  });
});
