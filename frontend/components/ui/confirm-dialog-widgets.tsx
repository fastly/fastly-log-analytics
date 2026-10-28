'use client'

import * as React from "react"
import { Check, Copy, HelpCircle, Info, type LucideIcon } from "lucide-react"

import { cn, RETENTION_OPTIONS } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { useCopyToClipboard } from "@/hooks/useSSE"

interface HelpDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  title: React.ReactNode
  icon?: React.ReactNode
  size?: "lg" | "xl"
  titleClassName?: string
  children: React.ReactNode
}

/**
 * Shared help-dialog shell used by AnalyticsCard, StatCard, and other card
 * primitives. Centralises the dialog sizing, padding, header layout, and
 * body typography so consumers only have to pass title + content.
 */
export function HelpDialog({
  open,
  onOpenChange,
  title,
  icon,
  size = "lg",
  titleClassName,
  children,
}: HelpDialogProps) {
  const widthClass = size === "xl" ? "max-w-xl p-6 md:p-8" : "max-w-lg p-6"

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className={cn(widthClass, "overflow-y-auto max-h-[90vh]")}>
        <DialogHeader>
          <DialogTitle className={cn("flex items-center gap-2", size === "xl" && "text-xl", titleClassName)}>
            {icon && <span className="text-primary">{icon}</span>}
            {title}
          </DialogTitle>
        </DialogHeader>
        <div className="text-sm text-muted-foreground leading-relaxed mt-2">
          {children}
        </div>
      </DialogContent>
    </Dialog>
  )
}

interface RetentionOption {
  readonly value: string
  readonly label: string
}

interface RetentionSelectProps {
  id?: string
  value: string
  onValueChange: (value: string) => void
  options?: readonly RetentionOption[]
  triggerClassName?: string
  itemClassName?: string
}

export function RetentionSelect({
  id,
  value,
  onValueChange,
  options = RETENTION_OPTIONS,
  triggerClassName = 'h-7 text-[11px]',
  itemClassName = 'text-[11px]',
}: RetentionSelectProps) {
  const labels = React.useMemo(
    () => Object.fromEntries(options.map((o) => [o.value, o.label])),
    [options],
  )
  return (
    <Select value={value} onValueChange={(v) => v && onValueChange(v)}>
      <SelectTrigger id={id} className={triggerClassName}>
        <SelectValue>{(val) => labels[String(val)] || val}</SelectValue>
      </SelectTrigger>
      <SelectContent>
        {options.map((o) => (
          <SelectItem key={o.value} value={o.value} className={itemClassName}>
            {o.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}

interface LabelWithInfoProps {
  label: string
  info?: React.ReactNode
  className?: string
  labelClassName?: string
  htmlFor?: string
}

export function LabelWithInfo({ label, info, className, labelClassName, htmlFor }: LabelWithInfoProps) {
  return (
    <div className={cn("flex items-center gap-1", className)}>
      <Label htmlFor={htmlFor} className={cn("text-xs font-semibold", labelClassName)}>
        {label}
      </Label>
      {info && (
        /* A-9 (a11y, WCAG 1.4.13): inherit the accessible 400/300
            defaults from tooltip.tsx — info-icon tooltips carry the
            explanatory text users may need time to read and reach. */
        <TooltipProvider>
          <Tooltip>
            {/* A-8 (a11y, WCAG 2.1.1): Button (not span) so keyboard users can
                tab to the info icon and trigger the tooltip on focus. */}
            <TooltipTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon-xs"
                  aria-label={`More info: ${label}`}
                  className="flex items-center text-muted-foreground hover:text-foreground"
                />
              }
            >
              <Info className="h-3.5 w-3.5 " />
            </TooltipTrigger>
            <TooltipContent side="right" className="max-w-xs text-xs">
              <p>{info}</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      )}
    </div>
  )
}

export function ReviewCard({ children, className }: { children: React.ReactNode, className?: string }) {
  return (
    <div className={cn("border rounded-xl p-4 bg-muted/10 flex flex-col gap-4 shadow-sm", className)}>
      {children}
    </div>
  )
}

export function ReviewHeader({ children, icon: Icon, className }: { children: React.ReactNode, icon?: LucideIcon, className?: string }) {
  return (
    <h4 className={cn("font-semibold text-sm flex items-center gap-2 text-foreground/80 shrink-0", className)}>
      {Icon && <Icon className="h-4 w-4" />}
      {children}
    </h4>
  )
}

export function ReviewContent({ children, className }: { children: React.ReactNode, className?: string }) {
  return (
    <div className={cn("text-xs flex flex-col gap-2 flex-1", className)}>
      {children}
    </div>
  )
}

interface ReviewItemProps {
  label: string
  value?: string | number | React.ReactNode
  children?: React.ReactNode
  className?: string
  variant?: 'default' | 'between'
}

export function ReviewItem({ label, value, children, className, variant = 'default' }: ReviewItemProps) {
  if (variant === 'between') {
    return (
      <div className={cn("flex items-center justify-between", className)}>
        <span>{label}</span>
        <div className="shrink-0">
          {value ?? children}
        </div>
      </div>
    )
  }

  return (
    <div className={cn("flex flex-col gap-1", className)}>
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium truncate">{value ?? children}</span>
    </div>
  )
}

interface CopyButtonProps {
  text: string
  label?: string
  copiedLabel?: string
  ariaLabel?: string
  copiedAriaLabel?: string
  title?: string
  variant?: React.ComponentProps<typeof Button>['variant']
  size?: React.ComponentProps<typeof Button>['size']
  className?: string
  iconClassName?: string
  timeoutMs?: number
  stopPropagation?: boolean
  'data-testid'?: string
}

export function CopyButton({
  text,
  label,
  copiedLabel = 'Copied',
  ariaLabel,
  copiedAriaLabel,
  title = 'Copy for AI Analysis',
  variant = 'ghost',
  size = 'icon',
  className = 'h-5 w-5 text-muted-foreground hover:text-foreground hover:bg-muted ml-1 shrink-0',
  iconClassName,
  timeoutMs = 2000,
  stopPropagation = false,
  'data-testid': dataTestId,
}: CopyButtonProps) {
  const { copied, copy } = useCopyToClipboard(timeoutMs)
  const effectiveAriaLabel = copied && copiedAriaLabel ? copiedAriaLabel : ariaLabel

  return (
    <Button
      variant={variant}
      size={size}
      aria-label={effectiveAriaLabel}
      data-testid={dataTestId}
      className={className}
      onClick={(e) => {
        if (stopPropagation) e.stopPropagation()
        copy(text)
      }}
      title={title}
    >
      {label ? (
        copied ? (
          <>
            <Check className={cn('h-3 w-3 mr-1', iconClassName)} /> {copiedLabel}
          </>
        ) : (
          <>
            <Copy className={cn('h-3 w-3 mr-1', iconClassName)} /> {label}
          </>
        )
      ) : copied ? (
        <Check className={cn('h-3 w-3', iconClassName, ariaLabel ? 'text-emerald-500' : 'text-green-500')} />
      ) : (
        <Copy className={cn('h-3 w-3', iconClassName)} />
      )}
    </Button>
  )
}

interface StatCardProps {
  title: string
  value: React.ReactNode
  sub: React.ReactNode
  icon?: React.ElementType
  iconClassName?: string
  loading?: boolean
  loadingMessage?: string
  tooltip?: string
  helpContent?: React.ReactNode
  helpTitle?: string
  className?: string
}

export function StatCard({
  title,
  value,
  sub,
  icon: Icon,
  iconClassName,
  loading,
  loadingMessage,
  tooltip,
  helpContent,
  helpTitle,
  className,
}: StatCardProps) {
  const [isHelpOpen, setIsHelpOpen] = React.useState(false)

  const helpTrigger = helpContent ? (
    <Button
      variant="ghost"
      size="icon"
      aria-label="About this metric"
      className="h-6 w-6 text-muted-foreground hover:text-foreground"
      onClick={() => setIsHelpOpen(true)}
      title="About this metric"
    >
      <HelpCircle className="h-4 w-4" />
    </Button>
  ) : tooltip ? (
    <Tooltip>
      {/* A-8 (a11y, WCAG 2.1.1): Button (not span) so keyboard users can
          tab to the help icon and trigger the tooltip on focus. */}
      <TooltipTrigger
        render={
          <Button
            variant="ghost"
            size="icon-xs"
            aria-label="More info"
            className="text-muted-foreground hover:text-foreground transition-colors shrink-0"
          />
        }
      >
        <HelpCircle className="h-4 w-4" />
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-[200px] text-xs font-normal">
        {tooltip}
      </TooltipContent>
    </Tooltip>
  ) : null

  return (
    <>
      <Card className={className}>
        <CardHeader className="flex flex-row items-start justify-between space-y-0 pb-2">
          <div className="flex items-center gap-2">
            {Icon && <Icon className={cn("h-4 w-4 text-primary", iconClassName)} />}
            <CardTitle className="text-sm font-medium">
              {title}
            </CardTitle>
          </div>
          {helpTrigger && <div className="flex items-center gap-2">{helpTrigger}</div>}
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="space-y-1">
              <Skeleton className="h-8 w-24 mb-1" />
              {loadingMessage && (
                <span className="text-[11px] text-muted-foreground animate-pulse block">
                  {loadingMessage}
                </span>
              )}
            </div>
          ) : (
            <div className="text-2xl font-bold">{value}</div>
          )}
          <div className="text-xs text-muted-foreground mt-1">{sub}</div>
        </CardContent>
      </Card>

      {helpContent && (
        <HelpDialog
          open={isHelpOpen}
          onOpenChange={setIsHelpOpen}
          title={helpTitle ?? title}
          size="xl"
        >
          {helpContent}
        </HelpDialog>
      )}
    </>
  )
}
