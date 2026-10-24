import { ApiError } from "./api";

/** Plain-words copy for a failed lookup: what happened, and what to do. */
export interface ErrorCopy {
  what: string;
  todo: string;
}

/** Keyed by problem code (last segment of the problem `type`). Only codes
 *  GET /containers/{lpn} can return are listed; anything else falls back on
 *  the HTTP status, then on a generic message. */
const BY_CODE: Record<string, (lpn: string) => ErrorCopy> = {
  "container-not-found": (lpn) => ({
    what: `No container has the license plate ${lpn}.`,
    todo:
      "Scan the carton label again and check the digits. A carton becomes a container once its receipt has been processed, so if it arrived minutes ago, try again shortly or look the receipt up in Inbound receiving.",
  }),
};

const BY_STATUS: Record<number, (lpn: string) => ErrorCopy> = {
  400: (lpn) => ({
    what: `${lpn} is not a valid LPN or SSCC.`,
    todo: "Use only letters, digits, '.', '_' and '-' (up to 64 characters), then search again.",
  }),
  500: () => ({
    what: "Inventory storage could not read the container.",
    todo: "Nothing was changed. Search again in a moment; if it keeps failing, tell the inventory team.",
  }),
  0: () => ({
    what: "Inventory storage cannot be reached.",
    todo: "Check your connection and search again. Nothing was changed.",
  }),
};

export function describeLookupError(err: ApiError, lpn: string): ErrorCopy {
  const make = BY_CODE[err.code] ?? BY_STATUS[err.status];
  if (make) return make(lpn);
  return {
    what: "The container lookup failed.",
    todo: "Search again; if it keeps failing, tell the inventory team.",
  };
}

/** Copy for an LPN that fails the pattern before any request is made. */
export function invalidLpnCopy(lpn: string): ErrorCopy {
  return BY_STATUS[400](lpn);
}
