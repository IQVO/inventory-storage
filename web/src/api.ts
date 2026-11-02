import { INVENTORY_API_BASE } from "./config";
import type { Container, Reservation } from "./types";

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

/** The collection envelope of the fleet REST API guideline (section 3.3):
 *  `nextCursor` is absent on the last page. */
export interface Page<T> {
  items: T[];
  nextCursor?: string;
}

/** Page size asked of a collection GET (the guideline's maximum) and the
 *  bound on how many pages one lookup follows before it gives up loudly. */
const PAGE_LIMIT = 500;
const MAX_PAGES = 100;

/** GET `url` and parse the JSON body; every failure is an ApiError. */
async function getJSON<T>(url: string, signal?: AbortSignal): Promise<T> {
  let res: Response;
  try {
    res = signal ? await fetch(url, { signal }) : await fetch(url);
  } catch (err) {
    if (signal?.aborted) throw err;
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
  return (await res.json()) as T;
}

/** GET /containers/{lpn}. Side-effect free. */
export async function getContainer(lpn: string): Promise<Container> {
  return getJSON<Container>(`${INVENTORY_API_BASE}/containers/${encodeURIComponent(lpn)}`);
}

/** GET /reservations?demandRef=. Reads the `items` of every page, following
 *  `nextCursor` until it is absent (guideline 5.2/5.3), so a demandRef with
 *  more reservations than one page is never silently truncated. */
export async function listReservationsByDemandRef(
  demandRef: string,
  signal?: AbortSignal,
): Promise<Reservation[]> {
  const all: Reservation[] = [];
  let cursor: string | undefined;
  for (let n = 0; n < MAX_PAGES; n++) {
    const query = new URLSearchParams({ demandRef, limit: String(PAGE_LIMIT) });
    if (cursor) query.set("cursor", cursor);
    const page = await getJSON<Page<Reservation>>(`${INVENTORY_API_BASE}/reservations?${query.toString()}`, signal);
    all.push(...page.items);
    if (!page.nextCursor) return all;
    cursor = page.nextCursor;
  }
  throw new ApiError(0, { detail: `more than ${MAX_PAGES} pages of reservations for ${demandRef}` }, "Too many pages");
}
