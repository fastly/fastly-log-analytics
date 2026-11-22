import { describeError, recordStoreError, storageEstimate } from "./file-store";

export const OPFS_DIRECTORY = "flp-parquet-opfs-v1";
const LOCK = "flp-parquet-opfs-v1";
const DEFAULT_BUDGET = 256 * 1024 * 1024;
const DEFAULT_FILE_BUDGET = 64 * 1024 * 1024;
const MAX_FILES = 128;
const MAX_ACTIVE_FILES = 8;
let activeFiles = 0;
const pinWaiters: Array<() => void> = [];
let opfsEpoch = 0;
const activeOpfsDiskWrites = new Set<Promise<unknown>>();

export function bumpOpfsEpoch(): void {
  opfsEpoch += 1;
}

function abortError(): DOMException {
  return new DOMException("The operation was aborted.", "AbortError");
}

function isCancellationError(error: unknown): boolean {
  if (error instanceof DOMException && error.name === "AbortError") return true;
  if (
    error instanceof Error &&
    (error.name === "AbortError" || error.message === "Cache cleared")
  ) {
    return true;
  }
  return false;
}

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
  private readonly readOnlyRoot:
    | (() => Promise<FileSystemDirectoryHandle | null>)
    | undefined;
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
    this.readOnlyRoot = options
      ? options.root
        ? async () => {
            try {
              return await options.root!();
            } catch (error) {
              if (missing(error)) return null;
              throw error;
            }
          }
        : undefined
      : typeof globalThis.navigator?.storage?.getDirectory === "function"
        ? async () => {
            try {
              return await (
                await navigator.storage.getDirectory()
              ).getDirectoryHandle(OPFS_DIRECTORY);
            } catch (error) {
              if (missing(error)) return null;
              throw error;
            }
          }
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

  private async available(callOpfsEpoch = opfsEpoch): Promise<boolean> {
    if (this.root && this.locks) return true;
    if (!this.reportedUnavailable && callOpfsEpoch === opfsEpoch) {
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
    const callOpfsEpoch = opfsEpoch;
    signal?.throwIfAborted();
    if (callOpfsEpoch !== opfsEpoch) throw abortError();
    if (!(await this.available(callOpfsEpoch))) return false;
    const id = await digest(JSON.stringify([this.scope, key]));
    try {
      return await this.locks!.request(
        LOCK,
        { mode: "shared", signal },
        async () => {
          signal?.throwIfAborted();
          if (callOpfsEpoch !== opfsEpoch) throw abortError();
          const root = await this.readOnlyRoot!();
          if (!root) return false;
          const found = await completeFile(root, id, bytes);
          signal?.throwIfAborted();
          if (callOpfsEpoch !== opfsEpoch) throw abortError();
          return found;
        },
      );
    } catch (error) {
      if (signal?.aborted) throw signal.reason;
      if (callOpfsEpoch !== opfsEpoch || isCancellationError(error)) {
        throw abortError();
      }
      await this.report(key, bytes, error);
      return false;
    }
  }

  async open(key: string, signal?: AbortSignal): Promise<PinnedFile | null> {
    const callOpfsEpoch = opfsEpoch;
    signal?.throwIfAborted();
    if (callOpfsEpoch !== opfsEpoch) throw abortError();
    if (!(await this.available(callOpfsEpoch))) return null;
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
                signal?.throwIfAborted();
                if (callOpfsEpoch !== opfsEpoch) throw abortError();
                const root = await this.readOnlyRoot!();
                if (!root) {
                  resolve(null);
                  return;
                }
                signal?.throwIfAborted();
                if (callOpfsEpoch !== opfsEpoch) throw abortError();
                const entry = await metadata(root, id);
                signal?.throwIfAborted();
                if (callOpfsEpoch !== opfsEpoch) throw abortError();
                if (!entry) {
                  resolve(null);
                  return;
                }
                const handle = await root.getFileHandle(`${id}.data`);
                if ((await handle.getFile()).size !== entry.bytes) {
                  resolve(null);
                  return;
                }
                if (callOpfsEpoch !== opfsEpoch) throw abortError();
                await writeMetadata(root, id, {
                  ...entry,
                  used: Date.now(),
                }).catch((error) =>
                  callOpfsEpoch === opfsEpoch && !isCancellationError(error)
                    ? this.report(key, entry.bytes, error)
                    : undefined,
                );
                signal?.throwIfAborted();
                if (callOpfsEpoch !== opfsEpoch) throw abortError();
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
          if (
            !signal?.aborted &&
            callOpfsEpoch === opfsEpoch &&
            !isCancellationError(error)
          ) {
            await this.report(key, 0, error);
          }
          if (signal?.aborted) reject(signal.reason);
          else if (callOpfsEpoch !== opfsEpoch || isCancellationError(error))
            reject(abortError());
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
    const callOpfsEpoch = opfsEpoch;
    const ensureCurrent = () => {
      signal?.throwIfAborted();
      if (callOpfsEpoch !== opfsEpoch) throw abortError();
    };
    ensureCurrent();
    if (!(await this.available(callOpfsEpoch)))
      throw new Error("OPFS persistence unavailable");
    ensureCurrent();
    if (!Number.isSafeInteger(bytes) || bytes <= 0 || bytes > this.fileBudget) {
      const error = new Error("Selected file exceeds the OPFS file budget");
      if (callOpfsEpoch === opfsEpoch && !signal?.aborted) {
        await this.report(key, bytes, error);
      }
      throw error;
    }
    const id = await digest(JSON.stringify([this.scope, key]));
    ensureCurrent();
    try {
      await this.locks!.request(
        `${LOCK}:download:${id}`,
        { mode: "exclusive", signal },
        async () => {
          ensureCurrent();
          const existingRoot = await this.readOnlyRoot!();
          if (existingRoot && (await completeFile(existingRoot, id, bytes))) {
            return;
          }
          ensureCurrent();
          const response = await fetchFile();
          try {
            ensureCurrent();
            const body = response.body;
            if (response.status !== 200 || !body) {
              throw new Error(
                `Selected file download failed (${response.status}); a complete response is required`,
              );
            }
            const writePhase = this.locks!.request(
              LOCK,
              { mode: "exclusive", signal },
              async () => {
                // Check epoch & signal BEFORE `this.root()` so OPFS_DIRECTORY is never re-created after logout.
                ensureCurrent();
                const root = await this.root!();
                ensureCurrent();
                // Cache contents and quota may change while HTTP is in flight.
                if (await completeFile(root, id, bytes)) return;
                const entries: [string, Entry][] = [];
                const directory = root as FileSystemDirectoryHandle & {
                  entries(): AsyncIterableIterator<[string, FileSystemHandle]>;
                };
                for await (const [name, handle] of directory.entries()) {
                  ensureCurrent();
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
                ensureCurrent();
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
                ensureCurrent();
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
                  void reader.cancel(signal?.reason).catch((error) => {
                    if (
                      callOpfsEpoch === opfsEpoch &&
                      !signal?.aborted &&
                      !isCancellationError(error)
                    ) {
                      return this.report(key, bytes, error);
                    }
                    return undefined;
                  });
                };
                signal?.addEventListener("abort", onAbort, { once: true });
                let received = 0;
                let closed = false;
                try {
                  for (;;) {
                    ensureCurrent();
                    const chunk = await reader.read();
                    if (chunk.done) break;
                    ensureCurrent();
                    received += chunk.value.byteLength;
                    if (received > bytes)
                      throw new Error(
                        "Downloaded file length exceeds manifest length",
                      );
                    await writer.write(chunk.value as FileSystemWriteChunkType);
                  }
                  ensureCurrent();
                  if (received !== bytes)
                    throw new Error(
                      "Downloaded file length does not match manifest length",
                    );
                  await writer.close();
                  closed = true;
                  ensureCurrent();
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
            activeOpfsDiskWrites.add(writePhase);
            try {
              await writePhase;
            } finally {
              activeOpfsDiskWrites.delete(writePhase);
            }
          } finally {
            if (!response.bodyUsed) await response.body?.cancel();
          }
        },
      );
    } catch (error) {
      if (
        !signal?.aborted &&
        callOpfsEpoch === opfsEpoch &&
        !isCancellationError(error)
      ) {
        await this.report(key, bytes, error);
      }
      throw error;
    }
  }
}

export async function clearOPFSParquetStore(): Promise<void> {
  opfsEpoch += 1;
  if (activeOpfsDiskWrites.size > 0) {
    await Promise.allSettled([...activeOpfsDiskWrites]);
  }
  if (!globalThis.navigator?.storage?.getDirectory) return;
  const removeDirectory = async () => {
    const root = await navigator.storage.getDirectory();
    try {
      await root.removeEntry(OPFS_DIRECTORY, { recursive: true });
    } catch (error) {
      if (!missing(error)) throw error;
    }
  };
  if (globalThis.navigator?.locks) {
    await navigator.locks.request(LOCK, { mode: "exclusive" }, removeDirectory);
  } else {
    await removeDirectory();
  }
}
