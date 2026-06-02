import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Position } from "./dag_defs.js";

/**
 * Greatest lower bound (meet) of the given positions, computed by folding a
 * pairwise meet over them. The pairwise meet of A and B is
 * findForkPosition(A, B).commonFrontier (the frontier of the shared history).
 *
 * The meet of a set equals the meet of its causally-minimal elements, so any
 * generating set works -- callers may fold directly over fork.common without
 * first reducing it to an antichain. Disconnected positions meet to the empty
 * set. Each fold step uses the (indexed) findForkPosition, so this avoids any
 * un-indexed predecessor walk.
 *
 * Future: a native N-ary meet at the index level would avoid the O(k) folds.
 */
export async function computeMeet(
    positions: Position[],
    pairwiseMeet: (a: Position, b: Position) => Promise<Position>,
): Promise<Position> {
    if (positions.length === 0) {
        return new Set<B64Hash>();
    }
    let acc: Position = positions[0];
    for (let i = 1; i < positions.length; i++) {
        if (acc.size === 0) break; // meet with the empty position stays empty
        acc = await pairwiseMeet(acc, positions[i]);
    }
    return acc;
}
