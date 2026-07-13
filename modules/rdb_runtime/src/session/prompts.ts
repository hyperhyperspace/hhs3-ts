import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";

export type AuthInteractionContext = {
    canPrompt?: () => boolean;
    confirmSignRetry?: (authorLabel: string, op: string) => Promise<boolean>;
    confirmRefUpdateUnlock?: (observerGroup: string, authorLabel: string) => Promise<void>;
    unlockIdentity?: (label: string) => Promise<OwnIdentity | undefined>;
    onProgress?: (line: string) => void;
};

export class KeyUnlockDeclinedError extends Error {
    constructor(readonly label: string) {
        super('unlock declined');
        this.name = 'KeyUnlockDeclinedError';
    }
}

export function canPromptForKeys(context?: AuthInteractionContext): boolean {
    return context?.canPrompt?.() === true;
}
