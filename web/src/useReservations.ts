import { useEffect, useState } from "react";
import { listReservationsByDemandRef } from "./api";
import type { Reservation } from "./types";

interface Settled {
  demandRef: string;
  data: Reservation[] | null;
  error: Error | null;
}

export interface ReservationsState {
  data: Reservation[] | null;
  loading: boolean;
  error: Error | null;
}

/** Every reservation raised against `demandRef` (all pages of
 *  GET /reservations), or nothing while `demandRef` is null. A result that
 *  belongs to a previous demandRef is never shown under the current one. */
export function useReservations(demandRef: string | null): ReservationsState {
  const [settled, setSettled] = useState<Settled | null>(null);

  useEffect(() => {
    if (demandRef === null) return;
    const controller = new AbortController();
    listReservationsByDemandRef(demandRef, controller.signal).then(
      (data) => setSettled({ demandRef, data, error: null }),
      (err: unknown) => {
        if (controller.signal.aborted) return;
        setSettled({ demandRef, data: null, error: err instanceof Error ? err : new Error(String(err)) });
      },
    );
    return () => controller.abort();
  }, [demandRef]);

  const current = settled !== null && settled.demandRef === demandRef ? settled : null;
  return {
    data: current?.data ?? null,
    error: current?.error ?? null,
    loading: demandRef !== null && current === null,
  };
}
