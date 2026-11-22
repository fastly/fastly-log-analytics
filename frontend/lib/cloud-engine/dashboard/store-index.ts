import { describeError, recordStoreError, storageEstimate } from "./file-store";

export const OPFS_DIRECTORY = "flp-parquet-opfs-v1";
const LOCK = "flp-parquet-opfs-v1";
const DEFAULT_BUDGET = 256 * 1024 * 1024;
const DEFAULT_FILE_BUDGET = 64 * 1024 * 1024;
const MAX_FILES = 128;
const MAX_ACTIVE_FILES = 8;
let activeFiles = 0;
const pinWaiters: Array<() => void> = [];

function acquirePin(signal?: AbortSignal): Promise<() => void> {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const grant = () => {
      signal?.removeEventListener("abort", abort);
      if (signal?.aborted) {
        reject(signal.reason);
        return;
      }
      activeFiles++;
      let done = false;
      resolve(() => {
        if (done) return;
        done = true;
        activeFiles--;
        while (activeFiles < MAX_ACTIVE_FILES && pinWaiters.length) {
          pinWaiters.shift()!();
        }
      });
    };
    const abort = () => {
      const index = pinWaiters.indexOf(grant);
      if (index >= 0) pinWaiters.splice(index, 1);
      reject(signal!.reason);
    };
    if (activeFiles < MAX_ACTIVE_FILES && pinWaiters.length === 0) grant();
    else {
      pinWaiters.push(grant);
      signal?.addEventListener("abort", abort, { once: true });
    }
  });
}

type Options = {
  root?: () => Promise<FileSystemDirectoryHandle>;
  locks?: LockManager;
  maxBytes?: number;
  maxFileBytes?: number;
};
type Entry = { bytes: number; used: number };
type PinnedFile = { handle: FileSystemFileHandle; release: () => void };

async function digest(value: string): Promise<string> {
  const hash = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(value),
  );
  return [...new Uint8Array(hash)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function missing(error: unknown): boolean {
  return error instanceof DOMException && error.name === "NotFoundError";
}

async function remove(root: FileSystemDirectoryHandle, name: string) {
  try {
    await root.removeEntry(name);
  } catch (error) {
    if (!missing(error)) throw error;
  }
}

async function metadata(
  root: FileSystemDirectoryHandle,
  id: string,
): Promise<Entry | null> {
  try {
    const file = await (await root.getFileHandle(`${id}.json`)).getFile();
    const entry = JSON.parse(await file.text()) as Entry;
    return Number.isSafeInteger(entry.bytes) &&
      entry.bytes > 0 &&
      Number.isFinite(entry.used)
      ? entry
      : null;
  } catch (error) {
    if (missing(error) || error instanceof SyntaxError) return null;
    throw error;
  }
}

async function writeMetadata(
  root: FileSystemDirectoryHandle,
  id: string,
  entry: Entry,
) {
  const writer = await (
    await root.getFileHandle(`${id}.json`, { create: true })
  ).createWritable();
  try {
    await writer.write(JSON.stringify(entry));
    await writer.close();
  } catch (error) {
    await writer.abort();
    throw error;
  }
}

async function completeFile(
  root: FileSystemDirectoryHandle,
  id: string,
  bytes: number,
): Promise<boolean> {
  const entry = await metadata(root, id);
  if (!entry || entry.bytes !== bytes) return false;
  try {
    return (
      (await (await root.getFileHandle(`${id}.data`)).getFile()).size === bytes
    );
  } catch (error) {
    if (missing(error)) return false;
    throw error;
  }
}

export class OPFSParquetStore {
  private readonly root: Options["root"];
  private readonly locks: LockManager | undefined;
  private readonly budget: number;
  private readonly fileBudget: number;
  private reportedUnavailable = false;

  constructor(
    private readonly scope: string,
    options?: Options,
  ) {
    this.root = options
      ? options.root
      : typeof globalThis.navigator?.storage?.getDirectory === "function"
        ? async () =>
            (await navigator.storage.getDirectory()).getDirectoryHandle(
              OPFS_DIRECTORY,
              { create: true },
            )
        : undefined;
    this.locks = options ? options.locks : globalThis.navigator?.locks;
    this.budget = options?.maxBytes ?? DEFAULT_BUDGET;
    this.fileBudget = Math.min(
      options?.maxFileBytes ?? DEFAULT_FILE_BUDGET,
      this.budget,
    );
  }

  private async report(key: string, bytes: number, error: unknown) {
    const estimate = await storageEstimate();
    await recordStoreError({
      at: Date.now(),
      cache: OPFS_DIRECTORY,
      key,
      bytes,
      message: describeError(error),
      ...estimate,
    });
  }

  private async available(): Promise<boolean> {
    if (this.root && this.locks) return true;
    if (!this.reportedUnavailable) {
      this.reportedUnavailable = true;
      await this.report(
        "",
        0,
        new Error(
          "OPFS persistence unavailable; using nonpersistent selective HTTP reads.",
        ),
      );
    }
    return false;
  }

  async has(
    key: string,
    bytes: number,
    signal?: AbortSignal,
  ): Promise<boolean> {
    signal?.throwIfAborted();
    if (!(await this.available())) return false;
    const id = await digest(JSON.stringify([this.scope, key]));
    try {
      return await this.locks!.request(
        LOCK,
        { mode: "shared", signal },
        async () => {
          signal?.throwIfAborted();
          const found = await completeFile(await this.root!(), id, bytes);
          signal?.throwIfAborted();
          return found;
        },
      );
    } catch (error) {
      if (signal?.aborted) throw signal.reason;
      await this.report(key, bytes, error);
      return false;
    }
  }

  async open(key: string, signal?: AbortSignal): Promise<PinnedFile | null> {
    signal?.throwIfAborted();
    if (!(await this.available())) return null;
    const id = await digest(JSON.stringify([this.scope, key]));
    const releasePin = await acquirePin(signal);
    let unlock!: () => void;
    const released = new Promise<void>((resolve) => {
      unlock = resolve;
    });
    return new Promise<PinnedFile | null>((resolve, reject) => {
      void Promise.resolve()
        .then(() =>
          this.locks!.request(LOCK, { mode: "shared", signal }, async () => {
            await this.locks!.request(
              `${LOCK}:${id}`,
              { mode: "exclusive", signal },
              async () => {
                const root = await this.root!();
                signal?.throwIfAborted();
                const entry = await metadata(root, id);
                signal?.throwIfAborted();
                if (!entry) {
                  resolve(null);
                  return;
                }
                const handle = await root.getFileHandle(`${id}.data`);
                if ((await handle.getFile()).size !== entry.bytes) {
                  resolve(null);
                  return;
                }
                await writeMetadata(root, id, {
                  ...entry,
                  used: Date.now(),
                }).catch((error) => this.report(key, entry.bytes, error));
                signal?.throwIfAborted();
                let done = false;
                resolve({
                  handle,
                  release: () => {
                    if (done) return;
                    done = true;
                    unlock();
                  },
                });
                await released;
              },
            );
          }),
        )
        .catch(async (error) => {
          await this.report(key, 0, error);
          if (signal?.aborted) reject(signal.reason);
          else resolve(null);
        })
        .finally(releasePin);
    });
  }

  async persist(
    key: string,
    bytes: number,
    fetchFile: () => Promise<Response>,
    signal?: AbortSignal,
  ): Promise<void> {
    if (!(await this.available()))
      throw new Error("OPFS persistence unavailable");
    if (!Number.isSafeInteger(bytes) || bytes <= 0 || bytes > this.fileBudget) {
      const error = new Error("Selected file exceeds the OPFS file budget");
      await this.report(key, bytes, error);
      throw error;
    }
    const id = await digest(JSON.stringify([this.scope, key]));
    try {
      await this.locks!.request(
        `${LOCK}:download:${id}`,
        { mode: "exclusive", signal },
        async () => {
          signal?.throwIfAborted();
          if (await completeFile(await this.root!(), id, bytes)) return;
          const response = await fetchFile();
          try {
            const body = response.body;
            if (response.status !== 200 || !body) {
              throw new Error(
                `Selected file download failed (${response.status}); a complete response is required`,
              );
            }
            await this.locks!.request(
              LOCK,
              { mode: "exclusive", signal },
              async () => {
                signal?.throwIfAborted();
                const root = await this.root!();
                // Cache contents and quota may change while HTTP is in flight.
                if (await completeFile(root, id, bytes)) return;
                const entries: [string, Entry][] = [];
                const directory = root as FileSystemDirectoryHandle & {
                  entries(): AsyncIterableIterator<[string, FileSystemHandle]>;
                };
                for await (const [name, handle] of directory.entries()) {
                  if (
                    handle.kind === "file" &&
                    /^[a-f0-9]{64}\.data$/.test(name)
                  ) {
                    if (!(await metadata(root, name.slice(0, -5))))
                      await remove(root, name);
                    continue;
                  }
                  if (
                    handle.kind !== "file" ||
                    !/^[a-f0-9]{64}\.json$/.test(name)
                  )
                    continue;
                  const entryId = name.slice(0, -5);
                  const entry = await metadata(root, entryId);
                  if (entry) entries.push([entryId, entry]);
                  else await remove(root, name);
                }
                entries.sort((a, b) => a[1].used - b[1].used);
                const estimate = await storageEstimate();
                const quotaRoom =
                  estimate.quota === null || estimate.usage === null
                    ? Infinity
                    : Math.max(0, estimate.quota * 0.8 - estimate.usage);
                let used = entries.reduce(
                  (sum, [, entry]) => sum + entry.bytes,
                  0,
                );
                let reclaimed = 0;
                while (
                  entries.length &&
                  (used + bytes > this.budget ||
                    entries.length >= MAX_FILES ||
                    quotaRoom + reclaimed < bytes)
                ) {
                  const [victim, entry] = entries.shift()!;
                  await remove(root, `${victim}.json`);
                  await remove(root, `${victim}.data`);
                  used -= entry.bytes;
                  reclaimed += entry.bytes;
                }
                if (used + bytes > this.budget || quotaRoom + reclaimed < bytes)
                  throw new Error("Insufficient OPFS storage quota");
                await remove(root, `${id}.json`);
                let writer: FileSystemWritableFileStream;
                try {
                  writer = await (
                    await root.getFileHandle(`${id}.data`, { create: true })
                  ).createWritable();
                } catch (error) {
                  await body.cancel();
                  await remove(root, `${id}.data`);
                  throw error;
                }
                const reader = body.getReader();
                const onAbort = () => {
                  void reader
                    .cancel(signal?.reason)
                    .catch((error) => this.report(key, bytes, error));
                };
                signal?.addEventListener("abort", onAbort, { once: true });
                let received = 0;
                let closed = false;
                try {
                  for (;;) {
                    signal?.throwIfAborted();
                    const chunk = await reader.read();
                    if (chunk.done) break;
                    received += chunk.value.byteLength;
                    if (received > bytes)
                      throw new Error(
                        "Downloaded file length exceeds manifest length",
                      );
                    await writer.write(chunk.value as FileSystemWriteChunkType);
                  }
                  signal?.throwIfAborted();
                  if (received !== bytes)
                    throw new Error(
                      "Downloaded file length does not match manifest length",
                    );
                  await writer.close();
                  closed = true;
                  signal?.throwIfAborted();
                  await writeMetadata(root, id, { bytes, used: Date.now() });
                } catch (error) {
                  const cleanup = await Promise.allSettled([
                    reader.cancel(),
                    ...(!closed ? [writer.abort()] : []),
                  ]);
                  await remove(root, `${id}.json`);
                  await remove(root, `${id}.data`);
                  const failed = cleanup.filter(
                    (result) => result.status === "rejected",
                  );
                  if (failed.length) {
                    throw new AggregateError(
                      [error, ...failed.map((result) => result.reason)],
                      "OPFS download and stream cleanup failed",
                    );
                  }
                  throw error;
                } finally {
                  signal?.removeEventListener("abort", onAbort);
                  reader.releaseLock();
                }
              },
            );
          } finally {
            if (!response.bodyUsed) await response.body?.cancel();
          }
        },
      );
    } catch (error) {
      await this.report(key, bytes, error);
      throw error;
    }
  }
}

export async function clearOPFSParquetStore(): Promise<void> {
  if (!globalThis.navigator?.storage?.getDirectory || !navigator.locks) return;
  await navigator.locks.request(LOCK, { mode: "exclusive" }, async () => {
    const root = await navigator.storage.getDirectory();
    try {
      await root.removeEntry(OPFS_DIRECTORY, { recursive: true });
    } catch (error) {
      if (!missing(error)) throw error;
    }
  });
}
