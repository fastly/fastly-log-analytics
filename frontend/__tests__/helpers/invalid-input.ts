/**
 * The single sanctioned type escape in the test tree. Use it ONLY to feed a
 * deliberately malformed value (a shape the compiler would reject) into code
 * under test to prove runtime handling. It returns `never`, so it is
 * assignable to whatever parameter the call site expects without inference
 * games. Not for silencing ordinary type mismatches: build those fixtures
 * with `partial` or typed payloads.
 */
export function invalidInput(value: unknown): never {
  return value as never
}
