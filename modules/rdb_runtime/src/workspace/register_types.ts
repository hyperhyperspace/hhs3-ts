import { rCatalogFactory, rDbFactory, rDeployGateFactory, rSchemaFactory, rTableGroupFactory } from "@hyper-hyper-space/hhs3_rdb";
import type { Replica } from "@hyper-hyper-space/hhs3_replica";
import { rSetFactory } from "@hyper-hyper-space/hhs3_std_types";

export function registerRdbTypes(replica: Replica): void {
    replica.registerType('hhs/rdb_v1', rDbFactory);
    replica.registerType('hhs/rschema_v1', rSchemaFactory);
    replica.registerType('hhs/rcatalog_v1', rCatalogFactory);
    replica.registerType('hhs/rtable_group_v1', rTableGroupFactory);
    replica.registerType('hhs/rdeploy_gate_v1', rDeployGateFactory);
    replica.registerType('hhs/rset_v1', rSetFactory);
}
