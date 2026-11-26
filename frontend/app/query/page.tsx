import { Suspense } from 'react'

import Loading from './loading'
import QueryClient from './_sections/QueryClient'

export {
  QueryClient,
  CodeEditor,
  buildStructuredSql,
  queryPageUrlParsers,
  RawSqlMode,
  QueryToolbar,
  ModeToggle,
  StructuredMode,
  ResultsTable,
  type CodeEditorHandle,
  type QueryMode,
} from './_sections/QueryClient'

export default function QueryPage() {
  return (
    <Suspense fallback={<Loading />}>
      <QueryClient />
    </Suspense>
  )
}
