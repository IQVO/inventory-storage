import { describe, expect, it, vi } from "vitest";
import { ApiError, LPN_PATTERN, getContainer, listReservationsByDemandRef } from "./api";
import { describeLookupError } from "./containerErrors";
import type { Reservation } from "./types";

describe("LPN_PATTERN", () => {
  it("matches the service's lpn rule", () => {
    expect(LPN_PATTERN.test("003400000000000017")).toBe(true);
    expect(LPN_PATTERN.test("LPN-WH1.01_a")).toBe(true);
    expect(LPN_PATTERN.test("")).toBe(false);
    expect(LPN_PATTERN.test("a b")).toBe(false);
    expect(LPN_PATTERN.test("a/b")).toBe(false);
    expect(LPN_PATTERN.test("x".repeat(65))).toBe(false);
  });
});

describe("getContainer", () => {
  it("url-encodes the lpn and maps problem+json to ApiError.code", async () => {
    const fetchMock = vi.fn(async () =>
      new Response(
        JSON.stringify({
          type: "https://errors.inventory-storage.warehouse-systems.dev/container-not-found",
          title: "Container not found",
          detail: "container not found",
        }),
        { status: 404, headers: { "Content-Type": "application/problem+json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const err = await getContainer("A:B").catch((e: unknown) => e);
    expect(String((fetchMock.mock.calls[0] as unknown[])[0])).toMatch(/\/containers\/A%3AB$/);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).code).toBe("container-not-found");
    expect((err as ApiError).status).toBe(404);
  });

  it("falls back on the status when the body is not JSON", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("bad gateway", { status: 502 })));
    const err = (await getContainer("X").catch((e: unknown) => e)) as ApiError;
    expect(err.code).toBe("");
    expect(describeLookupError(err, "X").what).toBe("The container lookup failed.");
  });
});

function reservation(id: string): Reservation {
  return {
    id,
    sku: "SKU-1",
    quantity: 1,
    demandRef: "order-42",
    status: "ACTIVE",
    allocations: [],
    createdAt: "2026-08-22T14:00:00Z",
    expiresAt: "2026-08-22T14:50:00Z",
  };
}

function json(body: unknown) {
  return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
}

describe("listReservationsByDemandRef", () => {
  it("reads items from every page, following nextCursor until it is absent", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), "http://inventory.test");
      switch (url.searchParams.get("cursor")) {
        case null:
          return json({ items: [reservation("res-1"), reservation("res-2")], nextCursor: "c1" });
        case "c1":
          return json({ items: [reservation("res-3")], nextCursor: "c2" });
        default:
          return json({ items: [reservation("res-4")] });
      }
    });
    vi.stubGlobal("fetch", fetchMock);

    const got = await listReservationsByDemandRef("order 42");

    expect(got.map((r) => r.id)).toEqual(["res-1", "res-2", "res-3", "res-4"]);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    const urls = fetchMock.mock.calls.map((c) => new URL(String(c[0]), "http://inventory.test"));
    expect(urls.every((u) => u.pathname.endsWith("/reservations"))).toBe(true);
    expect(urls.every((u) => u.searchParams.get("demandRef") === "order 42")).toBe(true);
    expect(urls.every((u) => u.searchParams.get("limit") === "500")).toBe(true);
    expect(urls.map((u) => u.searchParams.get("cursor"))).toEqual([null, "c1", "c2"]);
  });

  it("returns an empty list for an empty first page", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => json({ items: [] })));
    expect(await listReservationsByDemandRef("nobody")).toEqual([]);
  });

  it("maps a problem+json answer to ApiError.code", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(
          JSON.stringify({
            type: "https://errors.inventory-storage.warehouse-systems.dev/invalid-cursor",
            title: "cursor is not a valid page token",
            detail: "cursor is not a valid page token",
          }),
          { status: 400, headers: { "Content-Type": "application/problem+json" } },
        ),
      ),
    );
    const err = (await listReservationsByDemandRef("order-42").catch((e: unknown) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(400);
    expect(err.code).toBe("invalid-cursor");
  });
});
