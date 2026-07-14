import type { ReplClient, ReplInteractions } from "./protocol.js";

type EntryKind = 'output' | 'error' | 'progress';

export async function mountRepl(client: ReplClient): Promise<void> {
    const form = element<HTMLFormElement>('command-form');
    const input = element<HTMLTextAreaElement>('command-input');
    const runButton = element<HTMLButtonElement>('run-command');
    const transcript = element<HTMLDivElement>('transcript');
    const terminal = document.querySelector<HTMLElement>('.terminal');
    const prompt = element<HTMLLabelElement>('session-prompt');
    const status = element<HTMLSpanElement>('runtime-status');
    const resetButton = element<HTMLButtonElement>('reset-runtime');
    const clearButton = element<HTMLButtonElement>('clear-output');
    const history: string[] = [];
    let historyIndex = 0;
    let busy = false;
    let activeProgressOutput: HTMLPreElement | undefined;

    const setBusy = (next: boolean): void => {
        busy = next;
        input.disabled = next;
        runButton.disabled = next;
        resetButton.disabled = next;
    };

    const setStatus = (text: string, kind: 'starting' | 'ready' | 'error'): void => {
        status.className = `status ${kind}`;
        const label = status.querySelector('span:last-child');
        if (label !== null) label.textContent = text;
    };

    const setPrompt = (text: string): void => {
        prompt.textContent = text;
    };

    const scrollToLatest = (): void => {
        requestAnimationFrame(() => {
            terminal?.scrollTo({ top: terminal.scrollHeight, behavior: 'smooth' });
        });
    };

    const appendEntry = (label: string, text: string, kind: EntryKind): HTMLPreElement | undefined => {
        if (text.length === 0) return undefined;
        const entry = document.createElement('article');
        entry.className = `entry entry-${kind}`;

        const entryLabel = document.createElement('div');
        entryLabel.className = 'entry-label';
        entryLabel.textContent = label;

        const content = document.createElement('pre');
        content.className = kind === 'output' || kind === 'error' ? 'entry-output' : 'entry-output';
        content.textContent = text;

        entry.append(entryLabel, content);
        transcript.append(entry);
        scrollToLatest();
        return content;
    };

    const appendCommand = (label: string, command: string): void => {
        const entry = document.createElement('article');
        entry.className = 'entry entry-input';

        const entryLabel = document.createElement('div');
        entryLabel.className = 'entry-label';
        entryLabel.textContent = label;

        const content = document.createElement('pre');
        content.className = 'entry-command';
        content.textContent = command;

        entry.append(entryLabel, content);
        transcript.append(entry);
        scrollToLatest();
    };

    const appendProgress = (text: string): void => {
        if (text.length === 0) return;
        if (activeProgressOutput === undefined) {
            activeProgressOutput = appendEntry('progress', text, 'progress');
            return;
        }
        activeProgressOutput.textContent += `\n${text}`;
        scrollToLatest();
    };

    const interactions: ReplInteractions = {
        requestPassphrase: (request) => showPassphraseDialog(request.title, request.detail),
        requestConfirmation: (request) => showConfirmationDialog(request.title, request.detail),
        onProgress: appendProgress,
    };

    const execute = async (): Promise<void> => {
        const command = input.value.trim();
        if (busy || command.length === 0) return;

        const activePrompt = prompt.textContent ?? 'rdb>';
        appendCommand(activePrompt, command);
        activeProgressOutput = undefined;
        history.push(command);
        historyIndex = history.length;
        input.value = '';
        setBusy(true);

        try {
            const result = await client.execute(command, interactions);
            if (result.output.length > 0) {
                appendEntry(result.exitCode === 0 ? 'result' : `error ${result.exitCode}`, result.output, result.exitCode === 0 ? 'output' : 'error');
            }
            setPrompt(result.prompt);
            if (result.quit) appendEntry('session', 'Use “Reset workspace” to start a new session.', 'output');
            setStatus('Memory workspace', 'ready');
        } catch (error) {
            appendEntry('error', errorMessage(error), 'error');
            setStatus('Runtime error', 'error');
        } finally {
            setBusy(false);
            input.focus();
        }
    };

    form.addEventListener('submit', (event) => {
        event.preventDefault();
        void execute();
    });

    input.addEventListener('keydown', (event) => {
        if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
            event.preventDefault();
            void execute();
            return;
        }
        if (event.key !== 'ArrowUp' && event.key !== 'ArrowDown') return;
        if (input.selectionStart !== 0 && input.selectionStart !== input.value.length) return;
        if (history.length === 0) return;

        event.preventDefault();
        if (event.key === 'ArrowUp') historyIndex = Math.max(0, historyIndex - 1);
        else historyIndex = Math.min(history.length, historyIndex + 1);
        input.value = historyIndex === history.length ? '' : history[historyIndex] ?? '';
        input.setSelectionRange(input.value.length, input.value.length);
    });

    clearButton.addEventListener('click', () => {
        transcript.replaceChildren();
        input.focus();
    });

    resetButton.addEventListener('click', async () => {
        if (busy) return;
        const confirmed = await showConfirmationDialog(
            'Reset workspace?',
            'All databases, keys, aliases, and command output in this tab will be discarded.',
        );
        if (!confirmed) return;

        setBusy(true);
        setStatus('Resetting', 'starting');
        try {
            setPrompt(await client.reset());
            transcript.replaceChildren();
            history.length = 0;
            historyIndex = 0;
            setStatus('Memory workspace', 'ready');
        } catch (error) {
            appendEntry('error', errorMessage(error), 'error');
            setStatus('Runtime error', 'error');
        } finally {
            setBusy(false);
            input.focus();
        }
    });

    try {
        setPrompt(await client.start());
        setStatus('Memory workspace', 'ready');
        input.focus();
    } catch (error) {
        setStatus('Unavailable', 'error');
        appendEntry('startup', errorMessage(error), 'error');
        setBusy(true);
    }

    window.addEventListener('pagehide', () => {
        void client.close();
    }, { once: true });
}

function showPassphraseDialog(title: string, detail: string): Promise<string | undefined> {
    return showDialog(title, detail, true).then((result) => result.confirmed ? result.value : undefined);
}

function showConfirmationDialog(title: string, detail: string): Promise<boolean> {
    return showDialog(title, detail, false).then((result) => result.confirmed);
}

function showDialog(
    title: string,
    detail: string,
    secret: boolean,
): Promise<{ confirmed: boolean; value: string }> {
    const dialog = element<HTMLDialogElement>('prompt-dialog');
    const heading = element<HTMLHeadingElement>('prompt-title');
    const description = element<HTMLParagraphElement>('prompt-detail');
    const input = element<HTMLInputElement>('prompt-value');
    const inputLabel = dialog.querySelector<HTMLLabelElement>('label[for="prompt-value"]');

    heading.textContent = title;
    description.textContent = detail;
    input.value = '';
    input.hidden = !secret;
    if (inputLabel !== null) inputLabel.hidden = !secret;

    return new Promise((resolve) => {
        const onInputKeyDown = (event: KeyboardEvent): void => {
            if (secret && event.key === 'Enter') {
                event.preventDefault();
                dialog.close('confirm');
            }
        };
        const onClose = (): void => {
            dialog.removeEventListener('close', onClose);
            input.removeEventListener('keydown', onInputKeyDown);
            resolve({
                confirmed: dialog.returnValue === 'confirm',
                value: input.value,
            });
        };
        dialog.addEventListener('close', onClose);
        input.addEventListener('keydown', onInputKeyDown);
        dialog.showModal();
        if (secret) input.focus();
    });
}

function element<T extends HTMLElement>(id: string): T {
    const found = document.getElementById(id);
    if (found === null) throw new Error(`Missing required element #${id}`);
    return found as T;
}

function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}
