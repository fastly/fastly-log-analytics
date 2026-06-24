import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Loader2 } from 'lucide-react'

export default function Loading() {
  return (
    <div className="min-h-screen flex justify-center bg-muted/40 p-6 pt-20">
      <Card className="w-full max-w-md">
        <CardHeader className="space-y-2">
          <div className="flex items-center gap-2">
            <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" aria-hidden="true" />
            <span className="text-sm text-muted-foreground" role="status">Loading…</span>
          </div>
        </CardHeader>
        <CardContent>
          <div className="h-32" aria-hidden="true" />
        </CardContent>
      </Card>
    </div>
  )
}
