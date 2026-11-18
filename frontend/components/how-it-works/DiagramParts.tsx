import { ArrowDown, ArrowRight, type LucideIcon } from "lucide-react";

export function DiagramNode({
  icon: Icon,
  title,
  detail,
  highlighted = false,
}: {
  icon: LucideIcon;
  title: string;
  detail: string;
  highlighted?: boolean;
}) {
  return (
    <div
      className={`relative h-full rounded-xl border p-4 ${
        highlighted ? "border-primary/40 bg-primary/5" : "border-border bg-card"
      }`}
    >
      <Icon className="mb-3 size-5 text-primary" aria-hidden="true" />
      <p className="text-sm font-semibold">{title}</p>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
        {detail}
      </p>
    </div>
  );
}

export function Connector({ label }: { label: string }) {
  return (
    <div className="flex items-center justify-center gap-2 py-2 text-muted-foreground lg:flex-col lg:px-1">
      <ArrowRight className="hidden size-4 lg:block" aria-hidden="true" />
      <ArrowDown className="size-4 lg:hidden" aria-hidden="true" />
      <span className="text-center font-mono text-[10px]">{label}</span>
    </div>
  );
}
