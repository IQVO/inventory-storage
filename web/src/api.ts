import { INVENTORY_API_BASE } from "./config";
import type { Container } from "./types";

/** RFC 7807 problem+json body inventory-storage returns on every error
 *  (`type` = https://errors.inventory-storage.warehouse-systems.dev/<code>). */
export interface ProblemDetails {
  type?: string;
  title?: string;
  status?: number;
  detail?: string;
  instance?: string;
}

/** Every failed call -- HTTP error with or without a problem body, or a
 *  network failure (status 0) -- surfaces as one ApiError. `code` is the last
 *  path segment of problem.type ("container-not-found"); "" when unknown. */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, problem: ProblemDetails | null, fallback: string) {
    super(problem?.detail || problem?.title || fallback);
    this.name = "ApiError";
    this.status = status;
    this.code = problem?.type ? (problem.type.split("/").pop() ?? "") : "";
  }
}

/** The service's own lpn rule (apis/openapi.yaml): 1..64 of [A-Za-z0-9._-]. */
export const LPN_PATTERN = /^[A-Za-z0-9._-]{1,64}$/;

/** GET /containers/{lpn}. Side-effect free. */
export async function getContainer(lpn: string): Promise<Container> {
  let res: Response;
  try {
    res = await fetch(`${INVENTORY_API_BASE}/containers/${encodeURIComponent(lpn)}`);
  } catch (err) {
    throw new ApiError(0, { detail: err instanceof Error ? err.message : String(err) }, "Network error");
  }
  if (!res.ok) {
    let problem: ProblemDetails | null = null;
    try {
      problem = (await res.json()) as ProblemDetails;
    } catch {
      // non-JSON error body: fall through with problem = null
    }
    throw new ApiError(res.status, problem, `${res.status} ${res.statusText}`.trim());
  }
  return (await res.json()) as Container;
}
