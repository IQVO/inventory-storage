import { describe, expect, it, vi } from "vitest";
import { ApiError, LPN_PATTERN, getContainer } from "./api";
import { describeLookupError } from "./containerErrors";

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
