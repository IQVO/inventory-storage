import { NavLink, Outlet, Route, Routes } from "react-router-dom";
import { ContainerLookupScreen } from "./screens/ContainerLookupScreen";
import { InventoryScreen } from "./screens/InventoryScreen";

const SUB_NAV = [
  { to: ".", label: "Usable inventory", end: true },
  { to: "containers", label: "Containers", end: false },
];

const linkStyle = ({ isActive }: { isActive: boolean }) => ({
  display: "inline-flex",
  padding: "6px 12px",
  borderRadius: "var(--wh-radius-pill)",
  fontSize: "var(--wh-font-size-sm)",
  fontWeight: isActive ? 600 : 500,
  color: isActive ? "var(--wh-color-text)" : "var(--wh-color-text-muted)",
  background: isActive ? "var(--wh-color-accent-muted)" : "transparent",
  textDecoration: "none",
});

/** Sub-nav rendered by a layout route with the path "/". The shell mounts this
 *  component inside `<Route path="/inventory/*">`; a relative NavLink rendered
 *  straight in that splat route would resolve against the FULL current URL
 *  (/inventory/containers + "containers"). A layout route WITH path "/" owns
 *  the mount point instead, under any prefix and standalone at `/`. */
function InventoryLayout() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-4)" }}>
      <nav aria-label="Inventory sections" style={{ display: "flex", gap: "var(--wh-space-2)" }}>
        {SUB_NAV.map((item) => (
          <NavLink key={item.to} to={item.to} end={item.end} style={linkStyle}>
            {item.label}
          </NavLink>
        ))}
      </nav>
      <Outlet />
    </div>
  );
}

/** Exposed as inventory_mfe/App via Module Federation. Routed under
 *  /inventory/* by the shell; relative routes work identically mounted under
 *  a prefix or at / (standalone dev, see main.tsx).
 *    /            usable-inventory + reservations lookup
 *    /containers  container lookup by LPN / SSCC (?lpn=) */
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<InventoryLayout />}>
        <Route index element={<InventoryScreen />} />
        <Route path="containers" element={<ContainerLookupScreen />} />
      </Route>
    </Routes>
  );
}
