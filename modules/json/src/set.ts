type JSONSet = {[key: string]: ''};

function createSet(): JSONSet {
    return {};
}

function addToSet(l: JSONSet, s: string) {
    l[s] = '';
}

function removeFromSet(l: JSONSet, s: string) {
    delete l[s];
}

function toSet(elements?: Iterable<string>): JSONSet {
    const literal: JSONSet = {};

    if (elements !== undefined) {
        for (const e of elements) {
            literal[e] = '';
        }
    }

    return literal;
}

function fromSet(l?: JSONSet): Iterable<string> {

    if (l !== undefined) {
        for (const v of Object.values(l)) {
            if (v !== '') {
                throw new Error('Malformed LiteralSet: ' + l.toString() + ' (all values should be empty strings, found "' + v + '" instead).');
            }
        }
    }

    return Object.keys(l || {});
}

function getSet(l: JSONSet): Set<string> {
    return new Set(fromSet(l));
}

function copySet(l: JSONSet): JSONSet {

    return Object.assign({}, l);
}

function setUnion(l1: JSONSet, l2: JSONSet): JSONSet {

    return Object.assign(Object.assign({}, l1), l2);
}

function setElements(l: JSONSet): string[] {
    return Object.keys(l);
}

function setSize(l: JSONSet): number {
    return Object.keys(l).length;
}

function isEmptySet(l: JSONSet): boolean {
    return Object.keys(l).length === 0;
}

export { JSONSet as Set, createSet, addToSet, removeFromSet, copySet, toSet, fromSet, getSet, setUnion, setElements, setSize, isEmptySet };