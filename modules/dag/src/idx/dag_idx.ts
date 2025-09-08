import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { FindForkPositionFn, FindMinimalCoverFn, Position } from "dag_defs";

export type DagIndex = {

  index(h: Hash, after?: Position): Promise<void> | void;

  findMinimalCover: FindMinimalCoverFn;
  findForkPosition: FindForkPositionFn;

  getIndexStore: () => Object;

};