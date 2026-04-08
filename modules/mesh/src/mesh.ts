// Top-level facade for a single network environment. Each Mesh instance owns
// its own ConnectionPool and uses a fixed set of transports, a single
// discovery service, and a single authenticator. Create one Mesh per network
// environment (e.g. local LAN, public internet, private device sync).

import type { PeerDiscovery } from './discovery.js';
import type { TopicId } from './discovery.js';
import type { PeerAuthenticator } from './authenticator.js';
import type { TransportProvider } from './transport.js';
import { ConnectionPool } from './connection_pool.js';
import { createSwarm, Swarm, SwarmMode } from './swarm.js';

export interface MeshConfig {
    transports:    TransportProvider[];
    discovery:     PeerDiscovery;
    authenticator: PeerAuthenticator;
}

export class Mesh {

    readonly pool: ConnectionPool;

    private config: MeshConfig;
    private activeSwarms: Swarm[] = [];
    private closed = false;

    constructor(config: MeshConfig) {
        this.config = config;
        this.pool = new ConnectionPool();
    }

    createSwarm(topic: TopicId, opts?: { targetPeers?: number; mode?: SwarmMode }): Swarm {
        if (this.closed) throw new Error('mesh is closed');

        const swarm = createSwarm(
            {
                topic,
                targetPeers: opts?.targetPeers,
                mode: opts?.mode,
            },
            {
                pool:          this.pool,
                discovery:     this.config.discovery,
                authenticator: this.config.authenticator,
                transports:    this.config.transports,
            },
        );

        this.activeSwarms.push(swarm);
        return swarm;
    }

    swarms(): Swarm[] {
        return [...this.activeSwarms];
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;

        for (const swarm of this.activeSwarms) {
            swarm.destroy();
        }
        this.activeSwarms = [];

        this.pool.close();

        for (const t of this.config.transports) {
            t.close();
        }
    }
}
