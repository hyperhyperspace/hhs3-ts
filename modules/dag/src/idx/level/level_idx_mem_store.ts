import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap } from "@hyper-hyper-space/hhs3_util";

import { EntryInfo, LevelIndexStore } from "./level_idx";
import { Position } from "../../dag_defs";


export class MemLevelIndexStore implements LevelIndexStore {


    levelFactor: number;

    nextTopoIndex: number = 0;
    props: Map<Hash, EntryInfo> = new Map();

    predLevels: Map<number, MultiMap<Hash, Hash>> = new Map();

    constructor(props?: {levelFactor?: number}) {
        this.levelFactor = props?.levelFactor || 64;
    }

    assignEntryInfo = async (node: Hash, after: Position) => {

        if (!this.props.has(node)) {
            const topoIndex = this.nextTopoIndex;
            let distanceToARoot = 0;
            for (const pred of after) {
                const predHeight = this.props.get(pred)?.distanceToARoot;

                if (predHeight === undefined) {
                    throw new Error('Attempted to index entry ' + node + ' but its predecessor ' + pred + ' is not indexed - impossible.');
                }

                const newHeight = predHeight + 1;

                if (distanceToARoot === 0 || newHeight < distanceToARoot) {
                    distanceToARoot = newHeight;
                }
            }
            this.nextTopoIndex = this.nextTopoIndex + 1;

            let level = 0;

            if (distanceToARoot === 0) {
                level = Number.MAX_SAFE_INTEGER;
            } else {
                let i = distanceToARoot;
                while (i>1 && i%this.levelFactor === 0) {
                    level = level + 1;
                    i = i / this.levelFactor;
                }
            }
            
            this.props.set(node, {topoIndex, level, distanceToARoot});

            return {topoIndex, level, distanceToARoot};
        } else {
            const p = this.props.get(node);
            return {topoIndex: p?.topoIndex!, level: p?.level!, distanceToARoot: p?.distanceToARoot!};
        }
    };
    
    getEntryInfo = async (node: Hash) => {
        const p = this.props.get(node);
        const topoIndex = p?.topoIndex!
        const level = p?.level!;
        const distanceToARoot = p?.distanceToARoot!;

        return {topoIndex, level, distanceToARoot};
    }

    addPred = async (level: number, node: Hash, pred: Hash) => {

        let predLevel = this.predLevels.get(level);

        if (predLevel === undefined) {
            predLevel = new MultiMap();
            this.predLevels.set(level, predLevel);
        }

        predLevel.add(node, pred);
    };

    getPreds = async (level:number, node: Hash) => this.predLevels.get(level)?.get(node)||new Set<Hash>();
}