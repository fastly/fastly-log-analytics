import { Suspense } from 'react'

import Loading from './loading'
import QueryClient from './_sections/QueryClient'

export {
  QueryClient,
  buildStructuredSql,
  queryPageUrlParsers,
  QueryToolbar,
  ModeToggle,
  StructuredMode,
  type QueryMode,
} from './_sections/QueryClient'
export { CodeEditor, RawSqlMode, type CodeEditorHandle } from './_sections/RawSqlEditor'
export { ResultsTable } from './_sections/QueryResultsTable'

export default function QueryPage() {
  return (
    <Suspense fallback={<Loading />}>
      <QueryClient />
    </Suspense>
  )
}
