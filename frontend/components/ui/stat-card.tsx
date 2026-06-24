import * as React from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { HelpDialog } from "@/components/ui/help-dialog"
import { Skeleton } from "@/components/ui/skeleton"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { Button } from "@/components/ui/button"
import { HelpCircle } from "lucide-react"
import { cn } from "@/lib/utils"

interface StatCardProps {
  title: string
  value: React.ReactNode
  sub: React.ReactNode
  icon?: React.ElementType
  iconClassName?: string
  loading?: boolean
  tooltip?: string
  helpContent?: React.ReactNode
  helpTitle?: string
}

export function StatCard({ title, value, sub, icon: Icon, iconClassName, loading, tooltip, helpContent, helpTitle }: StatCardProps) {
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
      <Card>
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
            <Skeleton className="h-8 w-24 mb-1" />
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
