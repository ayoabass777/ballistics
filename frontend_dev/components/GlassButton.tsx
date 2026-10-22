import * as React from "react"
import { Slot } from "@radix-ui/react-slot"
import { cva, type VariantProps } from "class-variance-authority"

import { cn } from "@/lib/utils"

const buttonVariants = cva(
  "inline-flex items-center justify-center whitespace-nowrap rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50",
  {
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground hover:bg-primary/90",
        destructive: "bg-destructive text-destructive-foreground hover:bg-destructive/90",
        outline: "border border-input bg-background hover:bg-accent hover:text-accent-foreground",
        secondary: "bg-secondary text-secondary-foreground hover:bg-secondary/80",
        ghost: "hover:bg-accent hover:text-accent-foreground",
        link: "text-primary underline-offset-4 hover:underline",
        primary:
          "relative overflow-hidden text-white font-semibold " +
          "bg-gradient-to-br from-blue-500 to-blue-700 " + // Blue gradient
          "border border-blue-400/50 shadow-lg " +
          "hover:from-blue-600 hover:to-blue-800 " +
          "active:from-blue-700 active:to-blue-900 " +
          "transition-all duration-200 ease-in-out " +
          "before:content-[''] before:absolute before:inset-0 " +
          "before:bg-gradient-to-br before:from-white/20 before:to-transparent " +
          "before:opacity-0 hover:before:opacity-100 before:transition-opacity before:duration-200",
        glass:
          "relative overflow-hidden text-white font-semibold " +
          "bg-gradient-to-br from-white/10 to-white/5 " +
          "backdrop-blur-lg border border-white/20 shadow-lg " +
          "hover:from-white/20 hover:to-white/10 " +
          "active:from-white/30 active:to-white/15 " +
          "transition-all duration-200 ease-in-out " +
          "before:content-[''] before:absolute before:inset-0 " +
          "before:bg-gradient-to-br before:from-white/30 before:to-transparent " +
          "before:opacity-0 hover:before:opacity-100 before:transition-opacity before:duration-200",
      },
      size: {
        default: "h-10 px-4 py-2",
        sm: "h-9 rounded-md px-3",
        lg: "h-11 rounded-md px-8",
        icon: "h-10 w-10",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
)

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean
  ariaPressed?: boolean
}

const GlassButton = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, asChild = false, ariaPressed, ...props }, ref) => {
    const Comp = asChild ? Slot : "button"
    return (
      <Comp
        className={cn(buttonVariants({ variant, size, className }))}
        ref={ref}
        aria-pressed={ariaPressed}
        {...props}
      />
    )
  },
)
GlassButton.displayName = "GlassButton"

export { GlassButton, buttonVariants }
