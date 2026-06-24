'use client'

import React from 'react'
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror'
import { sql } from '@codemirror/lang-sql'
import { keymap, EditorView } from '@codemirror/view'
import { indentWithTab } from '@codemirror/commands'
import { useTheme } from 'next-themes'

export interface CodeEditorHandle {
  /**
   * Insert the given text at the current cursor position, focus the editor,
   * and scroll the insertion into view. Powers the schema-browser
   * click-to-insert affordance on /query raw mode.
   */
  insertAtCursor: (text: string) => void
}

interface CodeEditorProps {
  value: string
  onChange: (value: string) => void
  schema?: { name: string; type: string }[]
  tableName?: string
  className?: string
  height?: string
  /** Accessible label announced to screen readers for the editor surface. */
  ariaLabel?: string
}

export const CodeEditor = React.forwardRef<CodeEditorHandle, CodeEditorProps>(function CodeEditor(
  {
    value,
    onChange,
    schema,
    tableName = 'logs',
    className,
    height = '300px',
    ariaLabel = 'SQL query editor',
  },
  ref,
) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'
  const cmRef = React.useRef<ReactCodeMirrorRef | null>(null)

  const schemaObj: Record<string, string[]> = {}
  if (schema) {
    const cols = schema.map(s => s.name)
    schemaObj[tableName] = cols
    if (tableName !== 'logs') schemaObj['logs'] = cols
  }

  React.useImperativeHandle(
    ref,
    () => ({
      insertAtCursor: (text: string) => {
        const view = cmRef.current?.view
        if (!view) return
        const { from, to } = view.state.selection.main
        view.dispatch({
          changes: { from, to, insert: text },
          selection: { anchor: from + text.length },
          scrollIntoView: true,
        })
        view.focus()
      },
    }),
    [],
  )

  // a11y: WCAG 2.1.1 / 2.1.2 — without this, Tab inside CodeMirror inserts a
  // tab character and Esc has no binding, so keyboard-only users get trapped
  // in the editor. We:
  //   1. Bind Tab to `indentWithTab` (Shift+Tab dedents) so indentation still
  //      works when the user is actively editing.
  //   2. Bind Escape to blur the contentDOM so keyboard users can always
  //      escape to the next focusable element via Tab afterwards.
  //   3. Expose an aria-label on the contenteditable surface (WCAG 4.1.2).
  const a11yExtensions = React.useMemo(
    () => [
      keymap.of([
        indentWithTab,
        {
          key: 'Escape',
          run: (view) => {
            view.contentDOM.blur()
            return true
          },
        },
      ]),
      EditorView.contentAttributes.of({ 'aria-label': ariaLabel }),
    ],
    [ariaLabel],
  )

  // defaultTable lets unqualified column names (e.g. `SELECT sta|`)
  // surface column completions without requiring `logs.` prefix —
  // matches what users get in Metabase / Snowsight.
  const sqlExtension = React.useMemo(
    () => sql({ schema: schemaObj, defaultTable: tableName }),
    // schemaObj is rebuilt every render; depend on its stable inputs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [schema, tableName],
  )

  return (
    <div className={className}>
      <CodeMirror
        ref={cmRef}
        value={value}
        height={height}
        theme={isDark ? 'dark' : 'light'}
        extensions={[sqlExtension, ...a11yExtensions]}
        onChange={onChange}
        className="border rounded-md overflow-hidden"
      />
      <p className="sr-only" id="code-editor-keyboard-hint">
        Press Escape, then Tab, to move focus out of the editor. Press Control-Space to trigger column autocomplete.
      </p>
    </div>
  )
})
