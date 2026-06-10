"use client"

import * as React from "react"
import { Select as SelectPrimitive } from "@base-ui/react/select"

import { cn } from "@/lib/utils"
import { ChevronDownIcon, CheckIcon, ChevronUpIcon } from "lucide-react"

function extractText(children: React.ReactNode): string {
  let text = '';
  React.Children.forEach(children, (child) => {
    if (typeof child === "string" || typeof child === "number") {
      text += child;
    } else if (React.isValidElement(child)) {
      text += extractText((child.props as any).children);
    }
  });
  return text;
}

function getLabelsFromChildren(children: React.ReactNode): Record<string, string> {
  const labels: Record<string, string> = {};
  
  function traverse(node: React.ReactNode) {
    React.Children.forEach(node, (child) => {
      if (!React.isValidElement(child)) return;
      
      const element = child as React.ReactElement<any>;
      if (element.props && element.props.value !== undefined) {
        labels[String(element.props.value)] = extractText(element.props.children);
      }
      
      if (element.props && element.props.children) {
        traverse(element.props.children);
      }
    });
  }
  
  traverse(children);
  return labels;
}

const SelectContext = React.createContext<{
  labels: Record<string, string>;
  registerLabel: (value: string, label: string) => void;
  unregisterLabel: (value: string) => void;
} | null>(null);

const Select = <Value extends string = string>({ children, ...props }: React.ComponentProps<typeof SelectPrimitive.Root<Value>> & { children?: React.ReactNode }) => {
  const initialLabels = React.useMemo(() => getLabelsFromChildren(children), [children]);
  const [dynamicLabels, setDynamicLabels] = React.useState<Record<string, string>>({});
  
  const registerLabel = React.useCallback((value: string, label: string) => {
    setDynamicLabels(prev => prev[value] === label ? prev : { ...prev, [value]: label });
  }, []);
  
  const unregisterLabel = React.useCallback((value: string) => {
    setDynamicLabels(prev => {
      const next = { ...prev };
      delete next[value];
      return next;
    });
  }, []);

  const labels = React.useMemo(() => ({ ...initialLabels, ...dynamicLabels }), [initialLabels, dynamicLabels]);

  return (
    <SelectContext.Provider value={{ labels, registerLabel, unregisterLabel }}>
      <SelectPrimitive.Root {...props as any}>{children}</SelectPrimitive.Root>
    </SelectContext.Provider>
  );
}

const SelectGroup = React.forwardRef<
  HTMLDivElement,
  SelectPrimitive.Group.Props
>(({ className, ...props }, ref) => (
  <SelectPrimitive.Group
    ref={ref}
    data-slot="select-group"
    className={cn("scroll-my-1 p-1", className)}
    {...props}
  />
))
SelectGroup.displayName = "SelectGroup"

const SelectValue = React.forwardRef<
  HTMLSpanElement,
  SelectPrimitive.Value.Props
>(({ className, placeholder, children, ...props }, ref) => {
  const ctx = React.useContext(SelectContext);

  return (
    <SelectPrimitive.Value
      ref={ref}
      data-slot="select-value"
      className={cn("flex flex-1 text-left *:data-[slot=select-value]:line-clamp-1", className)}
      {...props}
    >
      {(val: any) => {
        if (val == null || val === '') return (placeholder as React.ReactNode) || null;
        if (children) {
          return typeof children === 'function' ? (children as any)(val) : children;
        }
        return ctx?.labels[String(val)] || String(val);
      }}
    </SelectPrimitive.Value>
  );
})
SelectValue.displayName = "SelectValue"

const SelectTrigger = React.forwardRef<
  HTMLButtonElement,
  SelectPrimitive.Trigger.Props & { size?: "sm" | "default" }
>(({ className, size = "default", children, ...props }, ref) => {
  // M-8 (a11y): BaseUI's <Select.Trigger> renders a <button> with no
  // discernible text when the selected value is just inner content; axe
  // reports ~131 critical button-name violations across the analyst +
  // admin nav. Default the aria-label to a generic "Select" when no
  // explicit aria-label / aria-labelledby is provided so the residual
  // count drops to near-zero without per-call-site code changes. Callers
  // can still override with a specific label (e.g. "Active service").
  const ariaLabel = props["aria-label"]
  const ariaLabelledby = props["aria-labelledby"]
  const labelProps = !ariaLabel && !ariaLabelledby ? { "aria-label": "Select" } : {}
  return (
    <SelectPrimitive.Trigger
      ref={ref}
      data-slot="select-trigger"
      data-size={size}
      className={cn(
        "flex w-fit items-center justify-between gap-1.5 rounded-lg border border-input bg-transparent py-2 pr-2 pl-2.5 text-sm whitespace-nowrap transition-colors outline-none select-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50 aria-invalid:border-destructive aria-invalid:ring-3 aria-invalid:ring-destructive/20 data-placeholder:text-muted-foreground data-[size=default]:h-8 data-[size=sm]:h-7 data-[size=sm]:rounded-[min(var(--radius-md),10px)] dark:bg-input/30 dark:hover:bg-input/50 dark:aria-invalid:border-destructive/50 dark:aria-invalid:ring-destructive/40 [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4",
        className
      )}
      {...labelProps}
      {...props}
    >
      {children}
      <SelectPrimitive.Icon
        render={
          <ChevronDownIcon className="pointer-events-none size-4 text-muted-foreground" />
        }
      />
    </SelectPrimitive.Trigger>
  )
})
SelectTrigger.displayName = "SelectTrigger"

const SelectContent = React.forwardRef<
  HTMLDivElement,
  SelectPrimitive.Popup.Props &
    Pick<
      SelectPrimitive.Positioner.Props,
      "align" | "alignOffset" | "side" | "sideOffset" | "alignItemWithTrigger"
    >
>(
  (
    {
      className,
      children,
      side = "bottom",
      sideOffset = 4,
      align = "center",
      alignOffset = 0,
      alignItemWithTrigger = true,
      ...props
    },
    ref
  ) => (
    <SelectPrimitive.Portal>
      <SelectPrimitive.Positioner
        side={side}
        sideOffset={sideOffset}
        align={align}
        alignOffset={alignOffset}
        alignItemWithTrigger={alignItemWithTrigger}
        className="isolate z-50"
      >
        <SelectPrimitive.Popup
          ref={ref}
          data-slot="select-content"
          data-align-trigger={alignItemWithTrigger}
          className={cn(
            "relative isolate z-50 max-h-(--available-height) w-(--anchor-width) min-w-36 origin-(--transform-origin) overflow-x-hidden overflow-y-auto rounded-lg bg-popover text-popover-foreground shadow-md ring-1 ring-foreground/10 duration-100 data-[align-trigger=true]:animate-none data-[side=bottom]:slide-in-from-top-2 data-[side=inline-end]:slide-in-from-left-2 data-[side=inline-start]:slide-in-from-right-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2 data-open:animate-in data-open:fade-in-0 data-open:zoom-in-95 data-closed:animate-out data-closed:fade-out-0 data-closed:zoom-out-95",
            className
          )}
          {...props}
        >
          <SelectScrollUpButton />
          <SelectPrimitive.List>{children}</SelectPrimitive.List>
          <SelectScrollDownButton />
        </SelectPrimitive.Popup>
      </SelectPrimitive.Positioner>
    </SelectPrimitive.Portal>
  )
)
SelectContent.displayName = "SelectContent"

const SelectLabel = React.forwardRef<
  HTMLDivElement,
  SelectPrimitive.GroupLabel.Props
>(({ className, ...props }, ref) => (
  <SelectPrimitive.GroupLabel
    ref={ref}
    data-slot="select-label"
    className={cn("px-1.5 py-1 text-xs text-muted-foreground", className)}
    {...props}
  />
))
SelectLabel.displayName = "SelectLabel"

const SelectItem = React.forwardRef<
  HTMLDivElement,
  SelectPrimitive.Item.Props
>(({ className, children, value, ...props }, ref) => {
  const ctx = React.useContext(SelectContext);
  const registerLabel = ctx?.registerLabel;
  const unregisterLabel = ctx?.unregisterLabel;
  
  // Extract text and memoize to ensure it's a stable primitive dependency
  const text = React.useMemo(() => extractText(children), [children]);

  React.useEffect(() => {
    if (registerLabel && unregisterLabel && value !== undefined) {
      registerLabel(String(value), text);
      return () => unregisterLabel(String(value));
    }
  }, [registerLabel, unregisterLabel, value, text]);

  return (
    <SelectPrimitive.Item
      ref={ref}
      value={value}
      data-slot="select-item"
      className={cn(
        "relative flex w-full cursor-pointer items-center gap-1.5 rounded-md py-1 pr-8 pl-1.5 text-sm outline-hidden select-none focus:bg-accent focus:text-accent-foreground not-data-[variant=destructive]:focus:**:text-accent-foreground data-disabled:pointer-events-none data-disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4",
        className
      )}
      {...props}
    >
      <SelectPrimitive.ItemText>{children}</SelectPrimitive.ItemText>
      <SelectPrimitive.ItemIndicator
        render={
          <span className="pointer-events-none absolute right-2 flex size-4 items-center justify-center" />
        }
      >
        <CheckIcon className="pointer-events-none" />
      </SelectPrimitive.ItemIndicator>
    </SelectPrimitive.Item>
  );
})
SelectItem.displayName = "SelectItem"

const SelectSeparator = React.forwardRef<
  HTMLDivElement,
  SelectPrimitive.Separator.Props
>(({ className, ...props }, ref) => (
  <SelectPrimitive.Separator
    ref={ref}
    data-slot="select-separator"
    className={cn("pointer-events-none -mx-1 my-1 h-px bg-border", className)}
    {...props}
  />
))
SelectSeparator.displayName = "SelectSeparator"

const SelectScrollUpButton = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<typeof SelectPrimitive.ScrollUpArrow>
>(({ className, ...props }, ref) => (
  <SelectPrimitive.ScrollUpArrow
    ref={ref}
    data-slot="select-scroll-up-button"
    className={cn(
      "top-0 z-10 flex w-full cursor-pointer items-center justify-center bg-popover py-1 [&_svg:not([class*='size-'])]:size-4",
      className
    )}
    {...props}
  >
    <ChevronUpIcon />
  </SelectPrimitive.ScrollUpArrow>
))
SelectScrollUpButton.displayName = "SelectScrollUpButton"

const SelectScrollDownButton = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<typeof SelectPrimitive.ScrollDownArrow>
>(({ className, ...props }, ref) => (
  <SelectPrimitive.ScrollDownArrow
    ref={ref}
    data-slot="select-scroll-down-button"
    className={cn(
      "bottom-0 z-10 flex w-full cursor-pointer items-center justify-center bg-popover py-1 [&_svg:not([class*='size-'])]:size-4",
      className
    )}
    {...props}
  >
    <ChevronDownIcon />
  </SelectPrimitive.ScrollDownArrow>
))
SelectScrollDownButton.displayName = "SelectScrollDownButton"

export {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectScrollDownButton,
  SelectScrollUpButton,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
}
