// Types for rugo_footer.mjs. The JSON shapes are rendered by rugo_wasm.cpp, the value
// mapping by rugo_footer.mjs `getter()` / `readList()` / `readAvColumn()` — keep in step.

/** Anything the readers can read from: total size plus a byte-range reader. */
export interface ByteSource {
  size: number;
  read(offset: number, length: number): Promise<Uint8Array>;
  /** Tail bytes already fetched (httpSource sets this from its first request). */
  prefetched?: Uint8Array;
}

/** Sources built by this module also record every [offset, length] read. */
export interface TrackedSource extends ByteSource {
  reads: Array<[number, number]>;
}

export interface HttpSource extends TrackedSource {
  url: string;
}

/**
 * A decoded value, by column type (null marks a null, at any level):
 * - 8/16/32-bit ints, floats, dates (days), 32-bit times: number
 * - int64 / uint64, timestamps, 64-bit times: bigint (the stored ints; the type gives the unit)
 * - bool: boolean
 * - varchar / string, enum: string
 * - binary / varbinary: Uint8Array
 * - decimal: string, scaled
 * - uuid: hyphenated hex string
 * - interval: { months, us }
 * - ORC struct / map, Avro record / map: JSON text string, as the native readers render them
 * - list / array: nested arrays at any depth (see ListValue)
 */
export type Value = number | bigint | boolean | string | Uint8Array | { months: bigint; us: bigint } | ListValue;

/**
 * One list value; [] is an empty list. Elements follow Value. Parquet list elements are
 * narrower: ints, floats, bools and strings only (string for array<varchar>/array<string>,
 * Uint8Array otherwise); decimal, uuid and interval elements are not decoded there.
 */
export type ListValue = Array<Value | null>;

/** Decoded columns keyed by name, plus the row count. */
export interface Batch {
  num_rows: number;
  columns: Record<string, Array<Value | null>>;
}

// ---- Parquet: Range reads of the footer and of each requested column chunk ----

export interface SchemaColumn {
  name: string;
  physical_type: string;
  logical_type: string;
  nullable: boolean;
}

export interface ColumnChunk {
  name: string;
  physical_type: string;
  logical_type: string;
  codec: string;
  encodings: string[];
  num_values: number;
  total_compressed_size: number;
  total_uncompressed_size: number;
  null_count: number;
  distinct_count: number;
  has_min: boolean;
  has_max: boolean;
  has_bloom_filter: boolean;
  has_page_index: boolean;
}

export interface RowGroup {
  num_rows: number;
  total_byte_size: number;
  total_compressed_size: number;
  file_offset: number;
  columns: ColumnChunk[];
}

export interface ParquetFooter {
  num_rows: number;
  num_row_groups: number;
  schema: SchemaColumn[];
  row_groups: RowGroup[];
}

export interface ParquetFile {
  footer: ParquetFooter;
  /**
   * Decodes `columns` (default: all) of row group `rg`, with one read per column chunk.
   * Rejects on an out-of-range row group, an unknown column, a closed file, or any decode failure.
   */
  readRowGroup(rg: number, columns?: string[]): Promise<Batch>;
  /** Frees the parsed footer in the wasm heap. Idempotent. */
  close(): void;
}

/** Parses the footer and keeps it open for readRowGroup. Call close() when done. */
export function openParquet(src: ByteSource): Promise<ParquetFile>;

/** Reads and parses the Parquet footer only. Rejects on non-Parquet or malformed input. */
export function readFooter(src: ByteSource): Promise<ParquetFooter>;

// ---- ORC and Avro: the WHOLE file is read into the wasm heap (one read; < 4 GB, ~2 GB in practice) ----

export interface OrcColumn {
  name: string;
  kind: string;
  readable: boolean;
  /** Why the column can't be read; present only when readable is false. */
  refusal?: string;
  nested: boolean;
}

export interface OrcMeta {
  num_rows: number;
  compression: string;
  /** Rows per stripe. */
  stripes: number[];
  columns: OrcColumn[];
}

export interface OrcFile {
  meta: OrcMeta;
  /**
   * Decodes `columns` (default: all) of `stripe`. Rejects on an out-of-range stripe, an
   * unknown column, a closed file, or any native refusal (message passed through).
   */
  readStripe(stripe: number, columns?: string[]): Promise<Batch>;
  /** Frees the file in the wasm heap. Idempotent. */
  close(): void;
}

/** Reads the whole ORC file into the wasm heap. Call close() when done. */
export function openOrc(src: ByteSource): Promise<OrcFile>;

export interface AvroMeta {
  /** The writer schema, parsed from JSON. */
  schema: unknown;
  codec: string;
  columns: string[];
  /** Rows per batch (whole blocks, up to 65,536 rows). */
  batches: number[];
  num_rows: number;
}

export interface AvroFile {
  meta: AvroMeta;
  /**
   * Decodes `columns` (default: all; dotted paths allowed, e.g. "Rocket.Name") of `batch`.
   * Rejects on an out-of-range batch, an unknown column, a closed file, or any native refusal.
   */
  readBatch(batch: number, columns?: string[]): Promise<Batch>;
  /** Frees the file in the wasm heap. Idempotent. */
  close(): void;
}

/** Reads the whole Avro file into the wasm heap. Call close() when done. */
export function openAvro(src: ByteSource): Promise<AvroFile>;

// ---- Sources and module ----

/** Local file: reads only the requested ranges via Blob.slice(). */
export function blobSource(blob: Blob): TrackedSource;

/** Remote file via HTTP Range. Rejects if the server ignores Range. */
export function httpSource(url: string): Promise<HttpSource>;

/** Loads and caches the wasm module; call early to overlap init with I/O. The value is internal. */
export function loadRugo(): Promise<unknown>;
