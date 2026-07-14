export type RequestId = string;

export type ReplRequest =
    | { id: RequestId; kind: 'execute'; text: string }
    | { id: RequestId; kind: 'reset' }
    | { id: RequestId; kind: 'prompt-response'; promptId: RequestId; value?: string; confirmed: boolean };

export type ReplPrompt =
    | {
        id: RequestId;
        kind: 'passphrase';
        title: string;
        detail: string;
        label: string;
    }
    | {
        id: RequestId;
        kind: 'confirm';
        title: string;
        detail: string;
    };

export type ReplResponse =
    | { requestId: RequestId; kind: 'ready'; prompt: string }
    | { requestId: RequestId; kind: 'progress'; text: string }
    | { requestId: RequestId; kind: 'prompt'; prompt: ReplPrompt }
    | {
        requestId: RequestId;
        kind: 'complete';
        output: string;
        exitCode: number;
        quit: boolean;
        prompt: string;
    }
    | { requestId: RequestId; kind: 'fatal'; message: string };

export type ReplInteractions = {
    requestPassphrase(prompt: Omit<Extract<ReplPrompt, { kind: 'passphrase' }>, 'id'>): Promise<string | undefined>;
    requestConfirmation(prompt: Omit<Extract<ReplPrompt, { kind: 'confirm' }>, 'id'>): Promise<boolean>;
    onProgress(text: string): void;
};

export type ExecuteResult = {
    output: string;
    exitCode: number;
    quit: boolean;
    prompt: string;
};

/**
 * Main-thread and future worker clients share this interface. ReplRequest and
 * ReplResponse above are deliberately data-only so a worker transport can
 * bridge interaction callbacks without changing the UI.
 */
export interface ReplClient {
    start(): Promise<string>;
    execute(text: string, interactions: ReplInteractions): Promise<ExecuteResult>;
    reset(): Promise<string>;
    close(): Promise<void>;
}
