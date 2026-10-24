'use client'

import * as React from 'react'
import { DataTable, type DataTableProps } from './DataTable'

type DataTableReadonlyProps<TData, TValue> = Omit<
  DataTableProps<TData, TValue>,
  'enableColumnReorder' | 'initialColumnOrder'
>

function DataTableReadonlyImpl<TData, TValue>(props: DataTableReadonlyProps<TData, TValue>) {
  return <DataTable {...props} enableColumnReorder={false} />
}

export const DataTableReadonly = React.memo(DataTableReadonlyImpl) as typeof DataTableReadonlyImpl
