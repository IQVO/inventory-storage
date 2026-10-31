/** Wire types mirroring inventory-storage's dto.go response shapes
 *  (usableInventoryResponse / reservationResponse / allocationResponse)
 *  exactly -- kept hand-in-sync with the Go DTOs rather than code-generated
 *  for v1; revisit with openapi-typescript once the OpenAPI spec is the
 *  enforced source of truth. */
export interface UsableInventory {
  sku: string;
  usable: number;
}

export interface Allocation {
  stockUnitId: string;
  quantity: number;
}

/** Reservation.Status values -- see reservation.go's Status enum:
 *  ACTIVE, CONFIRMED, REVOKED, EXPIRED. */
export type ReservationStatus = "ACTIVE" | "CONFIRMED" | "REVOKED" | "EXPIRED";

export interface Reservation {
  id: string;
  sku: string;
  quantity: number;
  demandRef: string;
  status: ReservationStatus;
  allocations: Allocation[];
  createdAt: string;
  expiresAt: string;
}

/** Container lifecycle (ADR 0045): a sealed case is broken SEALED -> OPEN ->
 *  EMPTY in one transaction, so a looked-up container is SEALED or EMPTY. */
export type ContainerState = "SEALED" | "OPEN" | "EMPTY";

/** GET /containers/{lpn} -- the `Container` schema of apis/openapi.yaml.
 *  `ownerRef`, `siteId`, `binId`, `lot`, `expiry` and `stockUnitId` are
 *  omitted by the service when unset (fleet-owned stock, not yet stowed,
 *  EMPTY, no lot...), so every consumer must tolerate their absence. */
export interface Container {
  lpn: string;
  sku: string;
  gtin: string;
  unitsPerPack: number;
  qtyEaches: number;
  ownerRef?: string | null;
  siteId?: string;
  binId?: string;
  state: ContainerState;
  lot?: string;
  expiry?: string;
  stockUnitId?: string;
  receiptId: string;
  lineNo: number;
}
