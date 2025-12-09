import { Literal } from "literal";

export enum Type {
    String = 'string',
    BoundedString = 'bstring',
    Int8 = 'int8',
    Int16 = 'int16',
    Int32 = 'int32',
    BoundedInt = 'bounded_int',
    Float64 = 'float64',
    BoundedFloat = 'bounded_float',
    Boolean = 'boolean',
    Array = 'array',
    BoundedArray = 'barray',
    FixedArray = 'farray',
    Object = 'object',
    Option = 'option',
    Union = 'union',
    Constant = 'literal',
    Something = 'something',
}

export type OptionFormat = Format | [Type.Option, Format];

export type Format =    Type.String |
                        [Type.BoundedString, number] |
                        Type.Int8 | Type.Int16 | Type.Int32 |
                        [Type.BoundedInt, number, number] |
                        Type.Float64 |
                        [Type.BoundedFloat, number, number] |
                        Type.Boolean |
                        [Type.Array, Format] |
                        [Type.BoundedArray, Format, number] |
                        [Type.FixedArray, Format, number] |
                        [Type.Union, Array<Format>] |
                        [Type.Constant, string|number|boolean] |
                        Type.Something |
                        {[key: string]: OptionFormat};

//string|number|Array<Literal>|{[key: string]: Literal};

function isOptionType(format: OptionFormat): boolean {

    if (Array.isArray(format) && format[0] === Type.Option) {
        if (format.length !== 2) {
            throw new Error('Invalid option format (expected 2 elements): ' + JSON.stringify(format));
        } else {
            return true;
        }
    } else {
        return false;
    }
}

export function checkFormat(format: Format, literal: Literal): boolean {

    if (Array.isArray(format)) {
        if (format.length < 1) {
            throw new Error('Invalid format: ' + JSON.stringify(format));
        } else {
            const t = format[0];

            switch (t) {
                case Type.BoundedString:
                    if (typeof(literal) !== 'string') {
                        return false;
                    } else {
                        return literal.length <= format[1];
                    }

                case Type.BoundedInt:

                    if (format.length !== 3) {
                        throw new Error('Invalid format (expected 3 elements): ' + JSON.stringify(format));
                    } else if (typeof(format[1]) !== 'number' || typeof(format[2]) !== 'number') {
                        throw new Error('Invalid format (expected number for int bounds in positions 1 and 2): ' + JSON.stringify(format));
                    } else if (!Number.isInteger(format[1]) || !Number.isInteger(format[2])) {
                        throw new Error('Invalid format (expected integer for int bounds in positions 1 and 2): ' + JSON.stringify(format));
                    } else if (typeof(literal) !== 'number') {
                        return false;
                    } else if (!Number.isInteger(literal)) {
                        return false;
                    } else if (literal < format[1] || literal > format[2]) {
                        return false;
                    } else {
                        return true;
                    }

                case Type.BoundedFloat:
                    if (format.length !== 3) {
                        throw new Error('Invalid format (expected 3 elements): ' + JSON.stringify(format));
                    } else if (typeof(format[1]) !== 'number' || typeof(format[2]) !== 'number') {
                        throw new Error('Invalid format (expected number for float bounds in positions 1 and 2): ' + JSON.stringify(format));
                    } else if (typeof(literal) !== 'number') {
                        return false;
                    } else {
                        return literal >= format[1] && literal <= format[2];
                    }

                case Type.Array:
                    if (format.length !== 2) {
                        throw new Error('Invalid format (expected 2 elements): ' + JSON.stringify(format));
                    } else {
                        if (!Array.isArray(literal)) {
                            return false;
                        } else {
                            return literal.every((item) => checkFormat(format[1], item));
                        }
                    }

                case Type.BoundedArray:
                    if (format.length !== 3) {
                        throw new Error('Invalid format (expected 3 elements): ' + JSON.stringify(format));
                    }/* else if (typeof(format[1]) !== 'object' || Array.isArray(format[1])) {
                        throw new Error('Invalid format (expected format for array elements in position 1): ' + JSON.stringify(format));
                    }*/ else if (typeof(format[2]) !== 'number') {
                        throw new Error('Invalid format (expected number for array length in position 2): ' + JSON.stringify(format));
                    } else if (!Number.isInteger(format[2])) {
                        throw new Error('Invalid format (expected integer for array length in position 2): ' + JSON.stringify(format));
                    } else if (!Array.isArray(literal)) {
                        return false;
                    }else {
                        return literal.length <= format[2] && literal.every((item) => checkFormat(format[1], item));
                    }

                case Type.FixedArray:
                    if (format.length !== 3) {
                        throw new Error('Invalid format (expected 3 elements): ' + JSON.stringify(format));
                    } else if (typeof(format[1]) !== 'number') {
                        throw new Error('Invalid format (expected number for tuple length in position 1): ' + JSON.stringify(format));
                    } else if (!Number.isInteger(format[1])) {
                        throw new Error('Invalid format (expected integer for tuple length in position 1): ' + JSON.stringify(format));
                    } else if (!Array.isArray(literal)) {
                        return false;
                    } else {
                        return literal.length === format[2] && literal.every((item) => checkFormat(format[1], item));
                    }

                case Type.Constant:
                    if (format.length !== 2) {
                        throw new Error('Invalid format (expected 2 elements): ' + JSON.stringify(format));
                    } else if (typeof(format[1]) !== 'string' && typeof(format[1]) !== 'number' && typeof(format[1]) !== 'boolean') {
                        throw new Error('Invalid format (expected string, number, or boolean for literal in position 1): ' + JSON.stringify(format));
                    } else {
                        return literal === format[1];
                    }
                case Type.Union:
                    if (format.length !== 2) {
                        throw new Error('Invalid format (expected 2 elements): ' + JSON.stringify(format));
                    } else if (!Array.isArray(format[1])) {
                        throw new Error('Invalid format (expected array for union elements in position 1): ' + JSON.stringify(format));
                    } else {
                        return format[1].some((item) => checkFormat(item, literal));
                    }
                default:
                    throw new Error('Invalid format (expected format for array elements in position 1): ' + JSON.stringify(format));
            }
        }
    } else if (typeof(format) === 'object') {

        if (typeof(literal) !== 'object' || Array.isArray(literal)) {
            return false;
        } else {
            for (const [key, keyFormatOpt] of Object.entries(format)) {

                let keyFormat: Format;
                let optional: boolean;

                if (isOptionType(keyFormatOpt)) {
                    keyFormat = keyFormatOpt[1] as Format;
                    optional = true;
                } else {
                    keyFormat = keyFormatOpt as Format;
                    optional = false;
                }

                if (literal.hasOwnProperty(key)) {
                    const value = literal[key];
                    if (!checkFormat(keyFormat, value)) {
                        return false;
                    }
                } else {
                    if (!optional) {
                        return false;
                    }
                }
            }
            return true;
        }
    } else if (typeof(format) === 'string') {
        if (format === Type.String) {
        
            return typeof(literal) === 'string';

        } else if (format === Type.Int8 || format === Type.Int16 || format === Type.Int32) {
        
            if (typeof(literal) !== 'number' || !Number.isInteger(literal)) {
                return false;
            } else {
                if (format === Type.Int8) {
                    return literal >= -128 && literal <= 127;
                } else if (format === Type.Int16) {
                    return literal >= -32768 && literal <= 32767;
                } else if (format === Type.Int32) {
                    return literal >= -2147483648 && literal <= 2147483647;
                } else {
                    return false;
                }
            }

        } else if (format === Type.Float64) {
        
            if (typeof(literal) !== 'number') {
                return false;
            } else {
                return true;
            }
        } else if (format === Type.Boolean) {
            return typeof(literal) === 'boolean';
        } else if (format === Type.Something) {
            return literal !== undefined && literal !== null;
        } else {
            throw new Error('Invaled format type: ' + JSON.stringify(format));
        }
    } else {
        throw new Error('Invalid format: ' + JSON.stringify(format));
    }

    
}


