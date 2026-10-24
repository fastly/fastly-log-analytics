/**
 * jsdom has no layout, so the real @tanstack/react-virtual virtualizer would
 * render zero rows for DataTable/DataTableReadonly. Use from a test file as
 *
 *   vi.mock('@tanstack/react-virtual', async () =>
 *     (await import('../helpers/virtual-mock')).reactVirtualModuleMock())
 *
 * (vi.mock is hoisted, so the factory has to be reached via a dynamic import.)
 */
export function reactVirtualModuleMock(rowHeight = 40) {
  return {
    useVirtualizer: (options: { count?: number }) => {
      const count = options.count ?? 0
      return {
        getVirtualItems: () =>
          Array.from({ length: count }).map((_, i) => ({
            index: i,
            key: i,
            start: i * rowHeight,
            end: (i + 1) * rowHeight,
            size: rowHeight,
          })),
        getTotalSize: () => count * rowHeight,
      }
    },
  }
}
