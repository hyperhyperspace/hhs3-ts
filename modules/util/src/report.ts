// Shared structured issue reporting. A single IssueReporter is threaded
// top-down through the mesh, swarm, and sync layers so failures of many
// kinds can be surfaced with a common shape. Every field is optional; a
// given emit site fills only what it knows.

export type IssueSeverity = 'low' | 'moderate' | 'high';

// Numeric ordering for threshold comparisons by policy consumers, e.g.
// `severityRank[report.severity ?? 'low'] >= severityRank['moderate']`.
export const severityRank: Record<IssueSeverity, number> = {
    low: 0,
    moderate: 1,
    high: 2,
};

export interface IssueReport {
    // Emitting layer: 'sync' | 'session' | 'swarm' | 'mesh' | ...
    source?: string;
    // Category: 'validation-failed' | 'timeout' | 'protocol' | 'hash-mismatch'
    // | 'connect-failed' | 'decode-failed' | 'send-closed' | 'send-error'
    // | 'unauthorized' | ...
    kind?: string;
    // Reaction hint for the mesh/swarm: high = terminate the peer, moderate =
    // consider termination if it recurs too often, low = safe to ignore. A
    // missing severity is treated as 'low'.
    severity?: IssueSeverity;
    // Hash of the offending op/entry.
    opHash?: string;
    // DAG / topic the issue relates to.
    dagId?: string;
    // Peer identity key id (mirrors SwarmPeer.keyId / TopicChannel.peerId).
    keyId?: string;
    // Peer network address (mirrors SwarmPeer.endpoint / NetworkAddress).
    endpoint?: string;
    // Machine-readable validation / error code.
    code?: string;
    // Human-readable explanation.
    message?: string;
    // Stack trace when available.
    stack?: string;
}

export type IssueReporter = (report: IssueReport) => void;
