import { rDbFactory, rSchemaFactory, rTableGroupFactory } from "@hyper-hyper-space/hhs3_rdb";
import type { Replica } from "@hyper-hyper-space/hhs3_replica";
import { rSetFactory } from "@hyper-hyper-space/hhs3_std_types";

export function registerRdbTypes(replica: Replica): void {
    replica.registerType('hhs/rdb_v1', rDbFactory);
    replica.registerType('hhs/rschema_v1', rSchemaFactory);
    replica.registerType('hhs/rtable_group_v1', rTableGroupFactory);
    replica.registerType('hhs/rset_v1', rSetFactory);
}
