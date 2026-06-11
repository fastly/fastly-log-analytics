'use client'

import React, { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { ChevronRight, ChevronDown, Folder, Cloud, HardDrive, FileJson, Loader2, Maximize2, Minimize2, RefreshCw, Download } from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { cn, formatBytes } from '@/lib/utils'
import { useQueryClient } from '@tanstack/react-query'
import type { components } from '@/types/api.generated'

type TreeNode = components['schemas']['TreeNode']

interface TreeProps {
  prefix: string
  level: number
  type: 'iceberg' | 'raw'
  forceExpand?: boolean
}

function Node({ node, prefix, level, type, forceExpand }: { node: TreeNode, prefix: string, level: number, type: 'iceberg' | 'raw', forceExpand?: boolean }) {
  const [isOpen, setIsOpen] = useState(false)
  const { full, abbr, relative } = useDateFormat()
  const { activeServiceId } = useServiceStore()

  useEffect(() => {
    if (forceExpand !== undefined) {
      setIsOpen(forceExpand)
    }
  }, [forceExpand])

  if (node.type === 'directory') {
    return (
      <div>
        <div
          className={cn(
            "flex items-center justify-between gap-2 py-1.5 hover:bg-muted/50 cursor-pointer rounded-md transition-colors group",
            level === 0 ? "font-medium" : "text-sm text-muted-foreground"
          )}
          style={{ paddingLeft: `${level * 16 + 8}px`, paddingRight: '8px' }}
          onClick={() => setIsOpen(!isOpen)}
        >
          <div className="flex items-center gap-2 min-w-0 flex-1">
            {isOpen ? <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />}
            <Folder className="h-4 w-4 shrink-0 text-blue-500/70 dark:text-blue-400/70" />
            <span className="truncate group-hover:text-foreground transition-colors">{node.name}</span>
          </div>
          <div className="flex items-center gap-4 shrink-0">
            {node.size != null && node.size > 0 && (
              <span className="text-xs font-mono text-muted-foreground tabular-nums w-20 text-right">
                {formatBytes(node.size)}
              </span>
            )}
            <div className="w-20 flex items-center justify-end gap-2">
              <Button
                variant="ghost"
                size="icon"
                aria-label="Download folder as ZIP"
                className="h-6 w-6 text-muted-foreground transition-opacity"
                onClick={(e) => {
                  e.stopPropagation()
                  window.open(`/api/download-folder?service=${activeServiceId}&root=${type}&prefix=${encodeURIComponent(node.prefix || (prefix + node.name))}`, '_blank')
                }}
                title="Download folder as ZIP"
              >
                <Download className="h-3.5 w-3.5" />
              </Button>
            </div>
          </div>
        </div>
        {isOpen && (
          <Tree prefix={`${prefix}${node.name}/`} level={level + 1} type={type} forceExpand={forceExpand} />
        )}
      </div>
    )
  }

  return (
    <div
      className="flex items-center justify-between gap-4 py-1 pr-2 hover:bg-muted/50 group rounded-md transition-colors"
      style={{ paddingLeft: `${level * 16 + 32}px` }}
    >
      <div className="flex items-center gap-2 min-w-0 flex-1">
        <FileJson className="h-4 w-4 shrink-0 text-muted-foreground opacity-70" />
        <span className="text-sm font-mono truncate text-muted-foreground group-hover:text-foreground transition-colors">
          {node.name}
        </span>
      </div>
      <div className="flex items-center gap-4 shrink-0">
        {node.size != null && (
          <span className="text-xs font-mono text-muted-foreground tabular-nums w-20 text-right">
            {formatBytes(node.size)}
          </span>
        )}
        {node.mtime && (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={
                <span className="hidden sm:inline-block text-xs text-muted-foreground w-[120px] text-right truncate ">
                  {relative(node.mtime)}
                </span>
              } />
              <TooltipContent className="text-xs">
                {full(node.mtime)} {abbr()}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        )}
        <div className="w-20 flex items-center justify-end gap-2">
          {node.sync_status === 'synced' && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-green-500/10 text-green-600 border-green-500/20 shadow-none"><Cloud className="w-3 h-3 mr-1"/> Synced</Badge>}
          {node.sync_status === 'local' && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-blue-500/10 text-blue-600 border-blue-500/20 shadow-none"><HardDrive className="w-3 h-3 mr-1"/> Local</Badge>}
          {node.sync_status === 'cloud' && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-purple-500/10 text-purple-600 border-purple-500/20 shadow-none"><Cloud className="w-3 h-3 mr-1"/> Cloud</Badge>}
          {!node.sync_status && node.is_cloud && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-purple-500/10 text-purple-600 border-purple-500/20 shadow-none"><Cloud className="w-3 h-3 mr-1"/> Cloud</Badge>}

          {node.key && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={`Download ${node.name}`}
              className="h-6 w-6 text-muted-foreground transition-opacity"
              onClick={(e) => {
                e.stopPropagation()
                window.open(`/api/download?service=${activeServiceId}&key=${encodeURIComponent(node.key as string)}`, '_blank')
              }}
              title="Download file"
            >
              <Download className="h-3.5 w-3.5" />
            </Button>
          )}
        </div>
      </div>
    </div>
  )
}

function Tree({ prefix, level, type, forceExpand }: TreeProps) {
  const { activeServiceId } = useServiceStore()

  const { data, isLoading, error } = useQuery({
    queryKey: ['admin', type === 'iceberg' ? 'iceberg-tree' : 'raw-tree', activeServiceId, prefix],
    queryFn: async () => {
      const endpoint = type === 'iceberg' ? "/api/admin/iceberg-tree" : "/api/admin/raw-tree"
      const { data } = await client.GET(endpoint as any, {
        params: { query: { prefix: prefix || undefined } }
      })
      return data
    },
    enabled: !!activeServiceId,
  })

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 py-2 text-sm text-muted-foreground animate-pulse" style={{ paddingLeft: `${level * 16 + 28}px` }}>
        <Loader2 className="h-3 w-3 animate-spin" /> Loading...
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="py-2 text-sm text-destructive" style={{ paddingLeft: `${level * 16 + 28}px` }}>
        Failed to load directory.
      </div>
    )
  }

  const nodes = (data as any)?.nodes || []

  if (nodes.length === 0) {
    return (
      <div className="py-2 text-xs text-muted-foreground italic" style={{ paddingLeft: `${level * 16 + 32}px` }}>
        Empty directory
      </div>
    )
  }

  return (
    <div className="flex flex-col">
      {nodes.map((node: TreeNode) => (
        <Node key={node.name} node={node} prefix={prefix} level={level} type={type} forceExpand={forceExpand} />
      ))}
    </div>
  )
}

export function FileBrowser({ type }: { type: 'iceberg' | 'raw' }) {
  const [forceExpand, setForceExpand] = useState<boolean | undefined>(undefined)
  const queryClient = useQueryClient()
  const [isRefreshing, setIsRefreshing] = useState(false)
  const { activeServiceId } = useServiceStore()

  const handleRefresh = async () => {
    setIsRefreshing(true)
    await queryClient.invalidateQueries({ queryKey: ['admin', type === 'iceberg' ? 'iceberg-tree' : 'raw-tree', activeServiceId] })
    setTimeout(() => setIsRefreshing(false), 500)
  }

  return (
    <div className="flex flex-col font-sans h-full relative">
      <div className="flex items-center justify-between px-4 py-2 border-b bg-muted/5 sticky top-0 z-10 backdrop-blur-sm">
        <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">{type === 'iceberg' ? 'Iceberg Data Lake' : 'Raw Archives'}</span>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            className="h-7 w-7 p-0 font-semibold shadow-none border-muted/60 bg-background"
            onClick={handleRefresh}
            title="Refresh file list"
          >
            <RefreshCw className={cn("h-3.5 w-3.5", isRefreshing && "animate-spin")} />
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="h-7 text-[10px] px-2 font-semibold shadow-none border-muted/60 bg-background"
            onClick={() => setForceExpand(prev => prev === undefined ? true : !prev)}
          >
            {forceExpand ? <Minimize2 className="h-3 w-3 mr-1" /> : <Maximize2 className="h-3 w-3 mr-1" />}
            {forceExpand ? 'Collapse All' : 'Expand All'}
          </Button>
        </div>
      </div>
      <div className="p-2 overflow-x-auto min-w-[500px]">
        <Tree prefix="" level={0} type={type} forceExpand={forceExpand} />
      </div>
    </div>
  )
}
