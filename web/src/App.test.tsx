import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import App from "./App";

// Mounted the way the console shell does: inside a splat route.
function mountUnderShell(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/inventory/*" element={<App />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("inventory_mfe App", () => {
  it("shows the usable-inventory screen at the mount point", () => {
    mountUnderShell("/inventory");
    expect(screen.getByRole("heading", { name: "Inventory" })).toBeInTheDocument();
  });

  it("shows the container lookup at /inventory/containers", () => {
    mountUnderShell("/inventory/containers");
    expect(screen.getByRole("heading", { name: "Container lookup" })).toBeInTheDocument();
  });

  it("sub-nav links resolve against the mount point, not the current URL", async () => {
    vi.stubGlobal("fetch", vi.fn());
    mountUnderShell("/inventory/containers");
    const nav = screen.getByRole("navigation", { name: "Inventory sections" });
    expect(nav.querySelector('a[href="/inventory/containers"]')).not.toBeNull();
    expect(nav.querySelector('a[href="/inventory"]')).not.toBeNull();
    await userEvent.setup().click(screen.getByRole("link", { name: "Usable inventory" }));
    expect(screen.getByRole("heading", { name: "Inventory" })).toBeInTheDocument();
  });
});
