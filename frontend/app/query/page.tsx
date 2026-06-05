'use client'

import React, { useState, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useFieldLabel } from '@/hooks/useFieldLabel'
import { CodeEditor } from '@/components/CodeEditor'
import { DataTable } from '@/components/DataTable'
import { Button, buttonVariants } from '@/components/ui/button'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from '@/components/ui/select'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
  DropdownMenuLabel,
  DropdownMenuGroup,
} from '@/components/ui/dropdown-menu'
import { Play, Search, AlertCircle, Clock, Database, ArrowUpDown, ArrowUp, ArrowDown, History, Bookmark, Download, X } from 'lucide-react'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { ColumnDef } from '@tanstack/react-table'
import { PageHeader } from '@/components/ui/page-header'
import { downloadAsCsv } from '@/lib/utils'

const HISTORY_KEY = 'fastly_qe_history';

export default function QueryPage() {
  const { activeServiceId } = useServiceStore()
  const { full, abbr, timeAgo } = useDateFormat()
  const [sql, setSql] = useState('SELECT * FROM logs LIMIT 100')
  const [maxRows, setMaxRows] = useState<number>(10000)
  const [explain, setExplain] = useState<boolean>(false)
  const [history, setHistory] = useState<{sql: string, ts: number}[]>([])
  
  const getFieldLabel = useFieldLabel()

  useEffect(() => {
    try {
      const stored = localStorage.getItem(HISTORY_KEY);
      if (stored) {
        setHistory(JSON.parse(stored));
      }
    } catch(e) {}
  }, [])

  const { data: schemaData } = useQuery({
    queryKey: ['admin', 'schema', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/schema", { signal })
      return data as any
    },
    enabled: !!activeServiceId
  })

  const { data: presets } = useQuery({
    queryKey: ['query', 'presets', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/presets", { signal })
      return data as any
    },
    enabled: !!activeServiceId
  })

  const queryMutation = useMutation({
    mutationFn: async (params: { sql: string, max_rows: number, explain: boolean }) => {
      const { data } = await client.POST("/api/query", { 
        body: params
      })
      return data as any
    },
  })

  const pushHistory = (sqlToRun: string) => {
    const updated = [
      { sql: sqlToRun, ts: Date.now() },
      ...history.filter(h => h.sql !== sqlToRun)
    ].slice(0, 20);
    setHistory(updated);
    try {
      localStorage.setItem(HISTORY_KEY, JSON.stringify(updated));
    } catch(e) {}
  }

  const handleRun = () => {
    if (sql.trim()) {
      pushHistory(sql)
      queryMutation.mutate({ sql, max_rows: maxRows, explain })
    }
  }

  const handleExportCSV = () => {
    if (!queryMutation.data?.data?.length) return;
    
    const data = queryMutation.data.data;
    const cols = queryMutation.data.columns || [];
    downloadAsCsv(data, cols, 'query_results.csv');
  }

  const removeHistoryItem = (e: React.MouseEvent, index: number) => {
    e.stopPropagation()
    const updated = [...history]
    updated.splice(index, 1)
    setHistory(updated)
    try {
      localStorage.setItem(HISTORY_KEY, JSON.stringify(updated));
    } catch(e) {}
  }

  const columns: ColumnDef<any>[] = React.useMemo(() => {
    if (!queryMutation.data?.columns) return []
    return queryMutation.data.columns.map((col: any) => ({
      id: col,
      accessorFn: (row: any) => row[col],
      meta: { label: getFieldLabel(col) },
      header: ({ column }: any) => {
        const isSorted = column.getIsSorted()
        return (
          <Button
            variant="ghost"
            onClick={() => column.toggleSorting(isSorted === "asc")}
            className="h-8 px-2 data-[state=open]:bg-accent hover:bg-muted font-mono text-xs flex items-center whitespace-nowrap"
          >
            {getFieldLabel(col)}
            {isSorted === "desc" ? (
              <ArrowDown className="ml-2 h-3 w-3" />
            ) : isSorted === "asc" ? (
              <ArrowUp className="ml-2 h-3 w-3" />
            ) : null}
          </Button>
        )
      },
      cell: ({ row }: { row: any }) => {
        const value = row.original[col]
        if ((col === 'timestamp' || col.endsWith('_at')) && value && typeof value === 'string' && value.includes('T')) {
          try {
            return <span className="text-xs font-mono">{full(value)} {abbr()}</span>
          } catch(e) {
            // fallback if it's not a valid date string
          }
        }
        return <span className="text-xs font-mono">{value !== null && value !== undefined ? String(value) : 'null'}</span>
      }
    }))
  }, [queryMutation.data?.columns, full, abbr, getFieldLabel])

  if (!activeServiceId) {
    return <NoServiceSelected icon={Search} message="Please select a service from the header to run queries." />
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Query Explorer"
        description="Execute custom SQL against your local DuckDB log cache."
      >
        <Button 
          onClick={handleRun} 
          disabled={queryMutation.isPending || !sql.trim()}
          size="lg"
        >
          {queryMutation.isPending ? (
            <Database className="h-4 w-4 mr-2 animate-spin" />
          ) : (
            <Play className="h-4 w-4 mr-2" />
          )}
          Run Query
        </Button>
      </PageHeader>

      <div className="border rounded-lg bg-card shadow-sm">
        <div className="flex items-center justify-between p-2 border-b bg-muted/30 flex-wrap gap-2">
          <div className="flex items-center gap-2">
            <DropdownMenu>
              <DropdownMenuTrigger className={buttonVariants({ variant: "outline", size: "sm", className: "h-8" })}>
                <span className="flex items-center">
                  <Bookmark className="w-3.5 h-3.5 mr-2 text-muted-foreground" />
                  Presets
                </span>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="start" className="w-[300px]">
                <DropdownMenuGroup>
                  <DropdownMenuLabel>Recommended Queries</DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  {presets?.length ? presets.map((p: any, i: number) => (
                    <DropdownMenuItem 
                      key={i} 
                      className="flex-col items-start cursor-pointer py-2"
                      onClick={() => setSql(p.sql)}
                    >
                      <div className="font-semibold text-sm">{p.name}</div>
                      <div className="text-xs text-muted-foreground mt-0.5">{p.description}</div>
                    </DropdownMenuItem>
                  )) : (
                    <div className="p-4 text-xs text-muted-foreground text-center italic">No presets available.</div>
                  )}
                </DropdownMenuGroup>
              </DropdownMenuContent>
            </DropdownMenu>

            <DropdownMenu>
              <DropdownMenuTrigger className={buttonVariants({ variant: "outline", size: "sm", className: "h-8" })}>
                <span className="flex items-center">
                  <History className="w-3.5 h-3.5 mr-2 text-muted-foreground" />
                  History
                </span>
              </DropdownMenuTrigger>

              <DropdownMenuContent align="start" className="w-[400px] max-h-[400px] overflow-y-auto">
                <DropdownMenuGroup>
                  <DropdownMenuLabel>Recent Queries</DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  {history.length ? history.map((h, i) => (
                    <DropdownMenuItem 
                      key={i} 
                      className="flex items-start justify-between cursor-pointer py-2 group"
                      onClick={() => setSql(h.sql)}
                    >
                      <div className="overflow-hidden flex-1 mr-4">
                        <div className="text-xs font-mono truncate">{h.sql.replace(/\s+/g, ' ').trim()}</div>
                        <div className="text-[10px] text-muted-foreground mt-1">{timeAgo(new Date(h.ts))}</div>
                      </div>
                      <Button 
                        variant="ghost" 
                        size="icon" 
                        className="h-5 w-5 opacity-0 group-hover:opacity-100 shrink-0" 
                        onClick={(e) => removeHistoryItem(e, i)}
                      >
                        <X className="h-3 w-3" />
                      </Button>
                    </DropdownMenuItem>
                  )) : (
                    <div className="p-4 text-xs text-muted-foreground text-center italic">No history found.</div>
                  )}
                </DropdownMenuGroup>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>

          <div className="flex items-center gap-4">
            <div className="flex items-center space-x-2">
              <Switch id="explain" checked={explain} onCheckedChange={setExplain} />
              <Label htmlFor="explain" className="text-xs cursor-pointer text-muted-foreground">Plan</Label>
            </div>
            
            <Select value={maxRows.toString()} onValueChange={v => setMaxRows(Number(v))}>
              <SelectTrigger className="h-8 w-[140px] text-xs">
                <SelectValue placeholder="Row limit" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="100">Fetch 100 rows</SelectItem>
                <SelectItem value="500">Fetch 500 rows</SelectItem>
                <SelectItem value="1000">Fetch 1,000 rows</SelectItem>
                <SelectItem value="5000">Fetch 5,000 rows</SelectItem>
                <SelectItem value="10000">Fetch 10,000 rows</SelectItem>
                <SelectItem value="50000">Fetch 50,000 rows</SelectItem>
              </SelectContent>
            </Select>

            {queryMutation.data?.data && queryMutation.data.data.length > 0 && (
              <Button variant="outline" size="sm" className="h-8" onClick={handleExportCSV}>
                <Download className="w-3.5 h-3.5 mr-2" />
                Export
              </Button>
            )}
          </div>
        </div>

        <CodeEditor
          value={sql}
          onChange={setSql}
          schema={schemaData?.schema}
          tableName={schemaData?.table_name}
          height="400px"
        />
      </div>

      {queryMutation.error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Query Error</AlertTitle>
          <AlertDescription className="font-mono text-xs whitespace-pre-wrap break-words mt-2">
            {queryMutation.error instanceof Error ? queryMutation.error.message : 'An unknown error occurred'}
          </AlertDescription>
        </Alert>
      )}

      {queryMutation.data?.explain_plan && (
        <Alert>
          <AlertTitle className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-2">Query Plan</AlertTitle>
          <AlertDescription>
            <pre className="text-xs font-mono bg-muted/50 p-4 rounded-md overflow-x-auto">
              {queryMutation.data.explain_plan}
            </pre>
          </AlertDescription>
        </Alert>
      )}

      {queryMutation.data && (
        <div className="space-y-4">
          <div className="flex items-center gap-4 text-xs text-muted-foreground px-1">
            <span className="flex items-center gap-1">
              <Database className="h-3 w-3" />
              {queryMutation.data.data?.length || 0} rows returned
              {queryMutation.data.truncated && (
                <span className="text-amber-500 font-semibold ml-1">
                  {queryMutation.data.total_rows && queryMutation.data.total_rows > 0
                    ? `(Truncated to ${queryMutation.data.data?.length} of ${queryMutation.data.total_rows.toLocaleString()})`
                    : `(Truncated to ${queryMutation.data.data?.length} — more available; add LIMIT to count)`}
                </span>
              )}
            </span>
            <span className="flex items-center gap-1">
              <Clock className="h-3 w-3" />
              {queryMutation.data.elapsed_ms}ms execution time
            </span>
          </div>
          
          <div className="border rounded-lg bg-card overflow-hidden">
            <DataTable 
              columns={columns} 
              data={queryMutation.data.data || []} 
              isLoading={queryMutation.isPending}
              initialSorting={[{ id: 'timestamp', desc: true }]}
            />
          </div>
        </div>
      )}
    </div>
  )
}
