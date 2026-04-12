import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { ObjectMap, RObject, RObjectInit, RObjectConfig, BasicProvider, RObjectTypeRegistry } from "@hyper-hyper-space/hhs3_mvt";

export { ObjectMap, RObject, RObjectInit, RObjectConfig, BasicProvider, RObjectTypeRegistry };

export class Replica<P extends BasicProvider = BasicProvider> implements ObjectMap {

    private registry: RObjectTypeRegistry<P>;
    private objects: Map<B64Hash, RObject> = new Map();
    config: RObjectConfig;

    // If id is missing, the object creation payload has not yet been validated.
    // In that case the provider will refuse to return any resources that consume 
    // significant resources, throwing an exception instead. This mode is intended
    // for use in validation only.
    private createProvider: (objectMap: ObjectMap, id?: B64Hash) => P;
    

    constructor(registry: RObjectTypeRegistry<P>, createProvider: (objectMap: ObjectMap, id?: B64Hash) => P, config: RObjectConfig = {}) {
        this.registry = registry;
        this.createProvider = createProvider;
        this.config = config;
    }

    getRegistry(): RObjectTypeRegistry<P> { return this.registry; }

    async getObject(id: B64Hash): Promise<RObject> {
        return this.objects.get(id)!;
    }

    async addObject(init: RObjectInit): Promise<B64Hash> {

        const factory = await this.registry.lookup(init.type);

        const id = await factory.computeRootObjectId(init.payload, this.createProvider(this));
        const provider = this.createProvider(this, id);
        const valid = await factory.validateCreationPayload(init.payload, provider);

        if (valid) {
            await factory.executeCreationPayload(init.payload, provider);
            this.objects.set(id, await factory.loadObject(id, provider));
            return id;
        } else {
            throw new Error('Invalid creation payload');
        }
    }
}
