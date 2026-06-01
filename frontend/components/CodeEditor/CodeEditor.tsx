'use client'

import React from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { sql } from '@codemirror/lang-sql'
import { useTheme } from 'next-themes'

interface CodeEditorProps {
  value: string
  onChange: (value: string) => void
  schema?: { name: string; type: string }[]
  tableName?: string
  className?: string
  height?: string
}

export function CodeEditor({ value, onChange, schema, tableName = 'logs', className, height = '300px' }: CodeEditorProps) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'

  const schemaObj: Record<string, string[]> = {}
  if (schema) {
    const cols = schema.map(s => s.name)
    schemaObj[tableName] = cols
    if (tableName !== 'logs') schemaObj['logs'] = cols
  }

  return (
    <div className={className}>
      <CodeMirror
        value={value}
        height={height}
        theme={isDark ? 'dark' : 'light'}
        extensions={[sql({ schema: schemaObj })]}
        onChange={onChange}
        className="border rounded-md overflow-hidden"
      />
    </div>
  )
}
