import { Window } from "happy-dom";

const window = new Window({ url: "http://localhost/" });
const target = globalThis as unknown as { [name: string]: unknown };
target.window = window;
target.document = window.document;
target.HTMLElement = window.HTMLElement;
target.Element = window.Element;
target.Node = window.Node;
target.DocumentFragment = window.DocumentFragment;
target.MutationObserver = window.MutationObserver;
target.MouseEvent = window.MouseEvent;
target.getComputedStyle = window.getComputedStyle.bind(window);
target.requestAnimationFrame = window.requestAnimationFrame.bind(window);
target.cancelAnimationFrame = window.cancelAnimationFrame.bind(window);
target.IS_REACT_ACT_ENVIRONMENT = true;
