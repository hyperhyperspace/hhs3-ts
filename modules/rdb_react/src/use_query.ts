import { useMemo, useSyncExternalStore } from "react";

import {
    TableQuery,
    type ProjectedRow,
    type QuerySnapshot,
    type QuerySubscription,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { useProjection } from "./context.js";

export function useQuery(query: TableQuery): QuerySnapshot<ProjectedRow[]>;
export function useQuery(query: { [name: string]: TableQuery }): QuerySnapshot<{ [name: string]: ProjectedRow[] }>;
export function useQuery(
    query: TableQuery | { [name: string]: TableQuery },
): QuerySnapshot<ProjectedRow[] | { [name: string]: ProjectedRow[] }> {
    const source = useProjection();
    const key = queryKey(query);
    const subscription = useMemo(
        () => source.monitor(query) as QuerySubscription<ProjectedRow[] | { [name: string]: ProjectedRow[] }>,
        [source, key],
    );
    return useSyncExternalStore(subscription.subscribe, subscription.getSnapshot, subscription.getSnapshot);
}

function queryKey(query: TableQuery | { [name: string]: TableQuery }): string {
    if (query instanceof TableQuery) return query.fingerprint();
    return JSON.stringify(Object.keys(query).sort().map((name) => [name, query[name]!.fingerprint()]));
}
