import { useMemo } from "react";

import { isWritableTarget, type RowWriter } from "@hyper-hyper-space/hhs3_rdb_adapter";

import { useProjection } from "./context.js";

// HIP-20's writer, opened from the ProjectionProvider's source, so that reads
// and writes always use the same target. It throws when the source cannot
// write. The memo only keeps the writer the same object across renders, so
// that callbacks built on it don't change: opening a writer starts nothing and
// holds nothing, so React may run it twice or throw it away, as it may with
// useQuery's subscription.
export function useWriter(): RowWriter {
    const source = useProjection();
    if (!isWritableTarget(source)) throw new Error("useWriter's source can't write: it has no openWriter");
    return useMemo(() => source.openWriter(), [source]);
}
