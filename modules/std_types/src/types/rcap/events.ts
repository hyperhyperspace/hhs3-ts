import { Event } from "@hyper-hyper-space/hhs3_mvt";

export type RCapAddIdentityEvent = Event & {
    type(): "add-identity";
    keyId(): string;
};

export type RCapGrantEvent = Event & {
    type(): "grant";
    grantee(): string;
    capName(): string;
};

export type RCapRevokeEvent = Event & {
    type(): "revoke";
    grantee(): string;
    capName(): string;
};

export type RCapCreateCapEvent = Event & {
    type(): "create-cap";
    capName(): string;
};

export type RCapDeleteCapEvent = Event & {
    type(): "delete-cap";
    capName(): string;
};

export type RCapEvent = RCapAddIdentityEvent | RCapGrantEvent | RCapRevokeEvent
                      | RCapCreateCapEvent | RCapDeleteCapEvent;
