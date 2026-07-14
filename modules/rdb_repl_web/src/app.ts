import type { ExecuteResult, ReplClient, ReplInteractions } from "./protocol.js";

type EntryKind = 'output' | 'error' | 'progress';

export type SchemaPreset = {
    id: string;
    sql: string;
};

export async function mountRepl(client: ReplClient, schemaPresets: SchemaPreset[] = []): Promise<void> {
    const form = element<HTMLFormElement>('command-form');
    const input = element<HTMLTextAreaElement>('command-input');
    const runButton = element<HTMLButtonElement>('run-command');
    const transcript = element<HTMLDivElement>('transcript');
    const terminal = document.querySelector<HTMLElement>('.terminal');
    const prompt = element<HTMLLabelElement>('session-prompt');
    const status = element<HTMLSpanElement>('runtime-status');
    const schemaMenu = element<HTMLDetailsElement>('schema-menu');
    const schemaButtons = Array.from(schemaMenu.querySelectorAll<HTMLButtonElement>('[data-schema]'));
    const presetsById = new Map(schemaPresets.map((preset) => [preset.id, preset]));
    const history: string[] = [];
    let historyIndex = 0;
    let busy = false;
    let activeProgressOutput: HTMLPreElement | undefined;

    const setBusy = (next: boolean): void => {
        busy = next;
        input.disabled = next;
        runButton.disabled = next;
        for (const button of schemaButtons) button.disabled = next;
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

    const runVisibleCommand = async (command: string, addToHistory: boolean): Promise<ExecuteResult> => {
        const activePrompt = prompt.textContent ?? 'rdb>';
        appendCommand(activePrompt, command);
        activeProgressOutput = undefined;
        if (addToHistory) {
            history.push(command);
            historyIndex = history.length;
        }

        const result = await client.execute(command, interactions);
        if (result.output.length > 0) {
            appendEntry(result.exitCode === 0 ? 'result' : `error ${result.exitCode}`, result.output, result.exitCode === 0 ? 'output' : 'error');
        }
        setPrompt(result.prompt);
        if (result.quit) appendEntry('session', 'Use “Reset workspace” to start a new session.', 'output');
        return result;
    };

    const execute = async (): Promise<void> => {
        const command = input.value.trim();
        if (busy || command.length === 0) return;

        input.value = '';
        setBusy(true);
        try {
            await runVisibleCommand(command, true);
            setStatus('Memory workspace', 'ready');
        } catch (error) {
            appendEntry('error', errorMessage(error), 'error');
            setStatus('Runtime error', 'error');
        } finally {
            setBusy(false);
            input.focus();
        }
    };

    for (const button of schemaButtons) {
        button.addEventListener('click', async () => {
            const preset = presetsById.get(button.dataset.schema ?? '');
            schemaMenu.removeAttribute('open');
            if (busy || preset === undefined) return;

            setBusy(true);
            try {
                if (!await client.hasKey('admin')) {
                    const created = await runVisibleCommand('\\key create admin', false);
                    if (created.exitCode !== 0) {
                        setStatus('Memory workspace', 'ready');
                        return;
                    }
                }

                const selected = await runVisibleCommand('\\author admin', false);
                if (selected.exitCode !== 0) {
                    setStatus('Memory workspace', 'ready');
                    return;
                }

                await runVisibleCommand(preset.sql.trim(), true);
                setStatus('Memory workspace', 'ready');
            } catch (error) {
                appendEntry('error', errorMessage(error), 'error');
                setStatus('Runtime error', 'error');
            } finally {
                setBusy(false);
                input.focus();
            }
        });
    }

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
