// Events are still unimplemented, but here are some stubs for now

import { json } from "@hyper-hyper-space/hhs3_json";
import { Event } from "@hyper-hyper-space/hhs3_mvt";

export type RAddEvent = Event & {
    type(): "add";
    element(): json.Literal;
}

export type RDeleteEvent = Event &{
    type(): "delete";
    element(): json.Literal;
}

export type RSetEvent = RAddEvent | RDeleteEvent;
