/** A listed file whose object the server no longer has (HTTP 404). */
export class MissingCloudFileError extends Error {
  constructor(readonly key: string) {
    super(`File ${key} is listed but missing (404)`);
    this.name = "MissingCloudFileError";
  }
}
