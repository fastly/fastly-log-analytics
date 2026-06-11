// Screen-reader-only table companion for PlotlyChart.
//
// Charts render as <canvas>/<svg> with no inherent semantic content —
// a screen reader hits the figure and has nothing to read out. The
// `.sr-only` class hides the table visually (clip + abs-position +
// width/height 1px) while keeping it in the accessibility tree, so
// assistive tech reads the numbers without the sighted user seeing
// a duplicate.
//
// The table itself is generic — caller passes a TableShape from
// tracesToTable(); this component owns the markup + caption fallback
// + aria attributes only.

import * as React from 'react'

import type { TableShape } from './tracesToTable'

interface ChartA11yTableProps {
  shape: TableShape
}

export const ChartA11yTable = React.memo(function ChartA11yTable({ shape }: ChartA11yTableProps) {
  if (shape.empty) {
    // Empty / unsupported trace shape — still emit something so a
    // screen reader doesn't land on an unannounced visual region.
    return (
      <div className="sr-only" aria-hidden="false">
        <p>{shape.title}: no readable data points available.</p>
      </div>
    )
  }

  return (
    <table className="sr-only" aria-hidden="false">
      <caption>{shape.title}</caption>
      <thead>
        <tr>
          {shape.columns.map((col, i) => (
            <th key={i} scope="col">
              {col}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {shape.rows.map((row, ri) => (
          <tr key={ri}>
            {row.map((cell, ci) => (
              <td key={ci}>{String(cell)}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
})
