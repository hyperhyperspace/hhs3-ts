// Questions for the user. The CLI asks on the terminal; tests script the
// answers. A prompter that isn't interactive can't ask anything.
export interface Prompter {
    readonly interactive: boolean;
    say(line: string): void;
    ask(question: string): Promise<string>;
    secret(question: string): Promise<string>;
    close(): void;
}

// Answers in order; `said` and `asked` record what the user would have seen.
export function scriptedPrompter(answers: string[]): Prompter & { said: string[]; asked: string[] } {
    const queue = [...answers];
    const said: string[] = [];
    const asked: string[] = [];
    const next = async (question: string): Promise<string> => {
        asked.push(question);
        const answer = queue.shift();
        if (answer === undefined) throw new Error(`no scripted answer for '${question}'`);
        return answer;
    };
    return {
        interactive: true,
        said,
        asked,
        say(line) { said.push(line); },
        ask: next,
        secret: next,
        close() {},
    };
}

export const NON_INTERACTIVE: Prompter = {
    interactive: false,
    say() {},
    async ask() { throw new Error('no terminal to ask on'); },
    async secret() { throw new Error('no terminal to ask on'); },
    close() {},
};
