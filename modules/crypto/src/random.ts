export function getInt(min:number, max: number) {
    const array = new Uint32Array(1);
    window.crypto.getRandomValues(array);

    // Scale to desired range
    return min + (array[0] % (max - min + 1));
}