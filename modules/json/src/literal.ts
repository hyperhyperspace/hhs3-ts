export type LiteralMap = {[key: string]: Literal};
export type Literal = boolean|string|number|Array<Literal>|LiteralMap;

export function hasKey(m: LiteralMap, key: string): boolean {
  return m.hasOwnProperty(key);
}

export function equals(a: Literal, b: Literal): boolean {
  // If same reference, they're equal
  if (a === b) return true;
  
  // Get types
  const typeA = typeof a;
  const typeB = typeof b;
  
  // If types differ, not equal
  if (typeA !== typeB) return false;
  
  // If both are primitives (string or number), they would have been equal by reference check
  if (typeA === 'string' || typeA === 'number' || typeA === 'boolean') return false;
  
  // Both must be objects at this point (arrays or objects)
  const isArrayA = Array.isArray(a);
  const isArrayB = Array.isArray(b);
  
  // If one is array and other isn't, not equal
  if (isArrayA !== isArrayB) return false;
  
  // Both are arrays
  if (isArrayA && isArrayB) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!equals(a[i], b[i])) return false;
    }
    return true;
  }
  
  // Both are objects (LiteralMap)
  const aMap = a as LiteralMap;
  const bMap = b as LiteralMap;
  
  const keysA = Object.keys(aMap);
  const keysB = Object.keys(bMap);
  
  // Check if they have the same number of keys
  if (keysA.length !== keysB.length) return false;
  
  // Check each key exists in both and values are equal
  for (const key of keysA) {
    if (!hasKey(bMap, key)) return false;
    if (!equals(aMap[key], bMap[key])) return false;
  }
  
  return true;
}