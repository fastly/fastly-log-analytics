'use client'

import React from 'react'
import { CodeEditor } from '@/components/CodeEditor'

interface RawSqlModeProps {
  rawSql: string
  onRawSqlChange: (sql: string) => void
  schema?: any
  tableName?: string
}

/**
 * Raw mode: a CodeEditor for free-form SQL. The user has full control of the
 * query string; results are sorted client-side so we don't silently rewrite
 * their SQL.
 */
export function RawSqlMode({ rawSql, onRawSqlChange, schema, tableName }: RawSqlModeProps) {
  return (
    <CodeEditor
      value={rawSql}
      onChange={onRawSqlChange}
      schema={schema}
      tableName={tableName}
      height="400px"
    />
  )
}
