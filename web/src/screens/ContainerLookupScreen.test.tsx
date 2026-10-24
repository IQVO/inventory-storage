import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { ContainerLookupScreen } from "./ContainerLookupScreen";
import type { Container } from "../types";

const SEALED: Container = {
  lpn: "003400000000000017",
  sku: "SKU-1",
  gtin: "24006381333935",
  unitsPerPack: 12,
  qtyEaches: 12,
  ownerRef: "seller-a",
  siteId: "WH1",
  binId: "WH1-STOR-AMB-A01-01-01-A",
  state: "SEALED",
  lot: "L1",
  expiry: "2027-03-01",
  stockUnitId: "su-1",
  receiptId: "rcpt-1",
  lineNo: 1,
};

function reply(status: number, body: unknown, type = "application/json") {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": type } });
}

function problem(status: number, code: string, title: string, detail: string) {
  return reply(
    status,
    { type: `https://errors.inventory-storage.warehouse-systems.dev/${code}`, title, status, detail },
    "application/problem+json",
  );
}

function mockFetch(handler: (url: string) => Response | Promise<Response>) {
  const fn = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => handler(String(input)));
  vi.stubGlobal("fetch", fn);
  return fn;
}

function mount(initial = "/containers") {
  return render(
    <MemoryRouter initialEntries={[initial]}>
      <ContainerLookupScreen />
    </MemoryRouter>,
  );
}

async function search(lpn: string) {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText("LPN or SSCC"), lpn);
  await user.click(screen.getByRole("button", { name: "Look up" }));
}

describe("ContainerLookupScreen", () => {
  it("starts idle and makes no request", () => {
    const fetchMock = mockFetch(() => reply(200, SEALED));
    mount();
    expect(screen.getByRole("heading", { name: "Container lookup" })).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("renders the container card for a sealed case", async () => {
    const fetchMock = mockFetch(() => reply(200, SEALED));
    mount();
    await search("003400000000000017");

    const card = (await screen.findByRole("heading", { name: "003400000000000017" })).closest("section")!;
    const c = within(card);
    expect(c.getByText("SEALED")).toBeInTheDocument();
    expect(c.getByText("SKU-1")).toBeInTheDocument();
    expect(c.getByText("24006381333935")).toBeInTheDocument();
    expect(c.getByText("12 eaches")).toBeInTheDocument();
    expect(c.getByText("WH1-STOR-AMB-A01-01-01-A")).toBeInTheDocument();
    expect(c.getByText("seller-a")).toBeInTheDocument();
    expect(c.getByRole("link", { name: "rcpt-1 · line 1" })).toHaveAttribute(
      "href",
      "/inbound-receiving/receipts/rcpt-1",
    );
    expect(String(fetchMock.mock.calls[0]![0])).toMatch(/\/containers\/003400000000000017$/);
    expect(fetchMock.mock.calls[0]![1]).toBeUndefined(); // read-only: plain GET
  });

  it("shows an EMPTY case with no bin and fleet-owned stock (ownerRef null)", async () => {
    mockFetch(() => reply(200, { ...SEALED, state: "EMPTY", binId: undefined, ownerRef: null }));
    mount();
    await search("003400000000000017");

    expect(await screen.findByText("EMPTY")).toBeInTheDocument();
    expect(screen.getByText(/Broken: its eaches are now loose stock/)).toBeInTheDocument();
    expect(screen.getByText("None (case is empty)")).toBeInTheDocument();
    expect(screen.getByText("Fleet-owned")).toBeInTheDocument();
  });

  it("says a not-yet-stowed container has no bin yet", async () => {
    mockFetch(() => reply(200, { ...SEALED, binId: undefined, siteId: undefined }));
    mount();
    await search("003400000000000017");
    expect(await screen.findByText("Not stowed yet")).toBeInTheDocument();
  });

  it("explains a 404 container-not-found with what happened and what to do", async () => {
    mockFetch(() => problem(404, "container-not-found", "Container not found", "container not found"));
    mount();
    await search("NOPE");

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("No container has the license plate NOPE.");
    expect(alert).toHaveTextContent("Scan the carton label again");
  });

  it("explains a 500 without leaking internals", async () => {
    mockFetch(() => problem(500, "internal-error", "Internal error", "pq: connection refused"));
    mount();
    await search("LPN-1");

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Inventory storage could not read the container.");
    expect(alert).not.toHaveTextContent("pq:");
  });

  it("explains a network failure", async () => {
    mockFetch(() => {
      throw new TypeError("Failed to fetch");
    });
    mount();
    await search("LPN-1");
    expect(await screen.findByRole("alert")).toHaveTextContent("Inventory storage cannot be reached.");
  });

  it("rejects an invalid LPN client-side without calling the API", async () => {
    const fetchMock = mockFetch(() => reply(200, SEALED));
    mount();
    await search("bad lpn/1");
    expect(await screen.findByRole("alert")).toHaveTextContent("is not a valid LPN or SSCC");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("looks up straight from a ?lpn= deep link", async () => {
    const fetchMock = mockFetch(() => reply(200, SEALED));
    mount("/containers?lpn=003400000000000017");
    expect(await screen.findByRole("heading", { name: "003400000000000017" })).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("offers no break or reserve controls (read-only screen)", async () => {
    mockFetch(() => reply(200, SEALED));
    mount("/containers?lpn=003400000000000017");
    await screen.findByRole("heading", { name: "003400000000000017" });
    expect(screen.queryByRole("button", { name: /break|reserve/i })).toBeNull();
  });
});
