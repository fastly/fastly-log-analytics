import { type LucideIcon } from 'lucide-react'

interface NoServiceSelectedProps {
  icon: LucideIcon
  message: string
  title?: string
}

export function NoServiceSelected({
  icon: Icon,
  message,
  title = 'No Service Selected',
}: NoServiceSelectedProps) {
  return (
    <div className="flex flex-col items-center justify-center h-[50vh] text-center">
      <Icon className="h-10 w-10 text-muted-foreground mb-4" />
      <h2 className="text-xl font-semibold">{title}</h2>
      <p className="text-muted-foreground">{message}</p>
    </div>
  )
}
