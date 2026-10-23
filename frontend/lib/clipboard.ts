/**
 * Copy text to the system clipboard, returning true on success and false on
 * failure (e.g. non-secure context or permission denied).
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    return false
  }
}
