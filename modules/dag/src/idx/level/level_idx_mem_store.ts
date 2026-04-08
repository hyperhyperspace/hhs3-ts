import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MultiMap } from "@hyper-hyper-space/hhs3_util";

import { EntryInfo, LevelIndexStore } from "./level_idx.js";
import { Position } from "../../dag_defs.js";


export class MemLevelIndexStore implements LevelIndexStore {

    levelFactor: number;

    nextTopoIndex: number = 0;
    props: Map<B64Hash, EntryInfo> = new Map();

    predLevels: Map<number, MultiMap<B64Hash, B64Hash>> = new Map();
    succLevels: Map<number, MultiMap<B64Hash, B64Hash>> = new Map();

    constructor(props?: {levelFactor?: number}) {
        this.levelFactor = props?.levelFactor || 64;
    }

    assignEntryInfo = async (node: B64Hash, after: Position) => {

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
    
    getEntryInfo = async (node: B64Hash) => {
        const p = this.props.get(node);
        const topoIndex = p?.topoIndex!
        const level = p?.level!;
        const distanceToARoot = p?.distanceToARoot!;

        return {topoIndex, level, distanceToARoot};
    }

    addPred = async (level: number, node: B64Hash, pred: B64Hash) => {

        let predLevel = this.predLevels.get(level);

        if (predLevel === undefined) {
            predLevel = new MultiMap();
            this.predLevels.set(level, predLevel);
        }

        predLevel.add(node, pred);
    };

    getPreds = async (level:number, node: B64Hash) => this.predLevels.get(level)?.get(node)||new Set<B64Hash>();

    addSucc = async (level: number, node: B64Hash, succ: B64Hash) => {

        let succLevel = this.succLevels.get(level);
        
        if (succLevel === undefined) {
            succLevel = new MultiMap();
            this.succLevels.set(level, succLevel);
        }
        succLevel.add(node, succ);
    };

    getSuccs = async (level: number, node: B64Hash) => this.succLevels.get(level)?.get(node)||new Set<B64Hash>();
}