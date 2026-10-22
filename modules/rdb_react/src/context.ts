import { createContext, createElement, useContext, type ReactNode } from "react";

import type { QuerySource } from "@hyper-hyper-space/hhs3_rdb_adapter";

const ProjectionContext = createContext<QuerySource | undefined>(undefined);

export function ProjectionProvider(props: { source: QuerySource; children?: ReactNode }) {
    return createElement(ProjectionContext.Provider, { value: props.source }, props.children);
}

export function useProjection(): QuerySource {
    const source = useContext(ProjectionContext);
    if (source === undefined) throw new Error("useQuery must be used inside ProjectionProvider");
    return source;
}
