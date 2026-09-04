// A Mesh instance represents a single network environment. MeshScope names the
// two built-in environments: `localhost` (same machine, loopback) and
// `internet` (public overlay). Factories map a scope to concrete listen
// addresses, tracker defaults, and transports.

export type MeshScope = 'localhost' | 'internet';
