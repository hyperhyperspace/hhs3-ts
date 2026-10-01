export const SOCKET_FILE = 'run/rhost.sock';

// Where a running host listens, given its absolute folder: a Unix socket under
// the folder, or on Windows a named pipe named after it.
export function socketPathFor(hostDir: string, platform: string): string {
    if (platform === 'win32') return `\\\\.\\pipe\\rhost-${hostDir.replace(/[\\/:]+/g, '-')}`;
    return hostDir.endsWith('/') ? hostDir + SOCKET_FILE : `${hostDir}/${SOCKET_FILE}`;
}
