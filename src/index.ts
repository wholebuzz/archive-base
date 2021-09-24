export const sorted = require('sorted-array-functions')

/**
 * Header for JSON blocks in the skip chain.
 */
export interface ArchiveBlockHeader {
  id: string
  db: string
  key: string
  value: string
  dataType: string
  dataSort: string
  nextBlocks: string[]
  numNextBlocks: number
  numNextItems: number
}

/**
 * Block base has `header`, `data`, and `version` metadata.
 */
export interface ArchiveBlock {
  header: ArchiveBlockHeader
  data: any[]
  version?: string | number
}

/**
 * Inherits [[ArchiveBlock]] overriding `data`.
 */
export interface ArchiveBlockOfType<X> extends ArchiveBlock {
  data: X[]
  nextBlocksFirstKey?: Array<Partial<X>>
  nextBlocksLastKey?: Array<Partial<X>>
}

/**
 * Tip record format.
 */
export interface ArchiveTipRecord {
  id: string
  db: string
  key: string
  value: string
  blockId: string
  version?: string | number
}

/**
 * Block fetching options.
 */
export interface ArchiveGetBlockOptions {
  forUpdate?: boolean
  reverse?: boolean
  version?: string | number
}

/**
 * Tip fetching options.
 */
export interface ArchiveGetTipOptions {
  forInsert?: boolean
  forCompact?: boolean
}

/**
 * Primary interface for block database. e.g. a key-value store, with value: { header, data[] }.
 */
export abstract class ArchiveBlockDatabase {
  /**
   * Returns the [[ArchiveBlock]] with `id`.
   * @param blockId The id of the block to fetch.
   * @param options
   */
  abstract getBlock(id: string, options?: ArchiveGetBlockOptions): Promise<ArchiveBlock | null>

  /**
   * Returns [[ArchiveBlockHeader]] and first element for block with `id`.
   * @param blockId The id of the block to fetch.
   */
  abstract getBlockHeader(blockId: string): Promise<ArchiveBlock | null>
}

/**
 * Primary interface for tip pointer database. e.g. a key-value store.
 */
export abstract class ArchiveTipDatabase {
  /**
   * Returns the tip block id for the index in `db` with `key` and `value`.
   */
  abstract getTip(
    db: string,
    key: string,
    value: string,
    options?: ArchiveGetTipOptions
  ): Promise<ArchiveTipRecord | null>
}

/**
 * Each property is indexed separately.
 */
export interface ArchiveIndex<X> {
  name: string
  getter: (a: X) => string[]
  sorter: (a: X, b: X) => number
  blockDatabase: ArchiveBlockDatabase
  tipDatabase: ArchiveTipDatabase
}

/**
 * Extends block with index references.
 */
export interface ArchiveIndexBlockOfType<X> extends ArchiveBlockOfType<X> {
  index: ArchiveIndex<X>
  indexValue: string
}

/**
 * Client archive database options.
 */
export interface ArchiveDatabaseOptions {
  /**
   * True when the archive blocks are already sorted.
   */
  strictOrdering?: boolean
  dispose?: () => void
}

/**
 * Primary interface for block-chain (as oppposed to blockchain) database built
 * on top of the [[ArchiveBlockDatabase]] and [[ArchiveTipDatabase]] interfaces.
 */
export class ArchiveDatabase<X> {
  constructor(
    public dbName: string,
    public indices: Array<ArchiveIndex<X>>,
    public options?: ArchiveDatabaseOptions
  ) {}

  async findTipBlock(indexKey: string, indexValue: string) {
    if (!this.options?.strictOrdering) throw new Error(`use getTipBlock() w/o strictOrdering`)
    const index = this.findIndex(indexKey)
    return this.getTipBlock(index, indexValue)
  }

  /**
   * Returns the tip block for the `index` with `indexValue`.
   */
  async getTipBlock(
    index: ArchiveIndex<X>,
    indexValue: string,
    options?: ArchiveGetBlockOptions
  ): Promise<ArchiveIndexBlockOfType<X> | null> {
    const tip = await index.tipDatabase.getTip(this.dbName, index.name, indexValue, {})
    if (!tip?.blockId) return null
    const block = await index.blockDatabase.getBlock(tip.blockId, options)
    if (!block) return null
    if (!this.options?.strictOrdering) block.data.sort(index.sorter)
    return { data: block.data, header: block.header, index, indexValue }
  }

  /**
   * Returns the next block pointed to by the input `block`.
   * @param block The block whose child should be returned.
   */
  async getNextBlock(
    block: ArchiveIndexBlockOfType<X>,
    height = 0,
    options?: ArchiveGetBlockOptions
  ): Promise<ArchiveIndexBlockOfType<X> | null> {
    if (!block.header.nextBlocks || block.header.nextBlocks.length < 1) return null
    const nextBlock = await block.index.blockDatabase.getBlock(
      block.header.nextBlocks[height],
      options
    )
    if (!nextBlock) return null
    if (!this.options?.strictOrdering) nextBlock?.data.sort(block.index.sorter)
    return {
      data: nextBlock.data,
      header: nextBlock.header,
      index: block.index,
      indexValue: block.indexValue,
    }
  }

  async findBlockFor(indexKey: string, indexValue: string, item: Partial<X>) {
    const index = this.findIndex(indexKey)
    return this.indexBlockFor(index, indexValue, item)
  }

  async indexBlockFor(index: ArchiveIndex<X>, indexValue: string, item: Partial<X>) {
    const options = { reverse: true }
    let block = await this.getTipBlock(index, indexValue, options)
    if (!block) return null
    let current = block.header
    let i = current.nextBlocks.length - 1
    let ascending = true
    for (; i >= 0; i--, ascending = false) {
      while (current.nextBlocks[i]) {
        let nextBlock = null
        if (block.nextBlocksFirstKey) {
          if (block.index.sorter(block.nextBlocksFirstKey[i] as X, item as X) >= 0) break
        } else {
          nextBlock = await this.getNextBlock(block, i, options)
          if (!nextBlock?.data?.length || block.index.sorter(nextBlock.data[0], item as X) >= 0) {
            break
          }
        }
        if (ascending && i < current.nextBlocks.length - 1) {
          i++
        } else {
          block = nextBlock ?? (await this.getNextBlock(block, i, options))
          if (!block) return null
          current = block.header
          i = Math.min(i, current.nextBlocks.length - 1)
        }
      }
    }
    return block
  }

  async findItem(indexKey: string, indexValue: string, item: Partial<X>) {
    const index = this.findIndex(indexKey)
    return this.indexItem(index, indexValue, item)
  }

  async indexItem(index: ArchiveIndex<X>, indexValue: string, item: Partial<X>) {
    const block = await this.indexBlockFor(index, indexValue, item)
    if (!block) return null
    const ind = sorted.eq(block.data, item, block.index.sorter)
    return ind < 0 ? null : block.data[ind]
  }

  findIndex(indexKey: string) {
    const indice = this.indices.findIndex((x) => x.name === indexKey)
    if (indice < 0) throw new Error(`unknown index ${indexKey}`)
    return this.indices[indice]
  }
}
