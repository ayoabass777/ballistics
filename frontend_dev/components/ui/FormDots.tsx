"use client"

import { cn } from "@/lib/utils"

type MatchResult = "W" | "D" | "L"

interface FormDotsProps {
  form: MatchResult[]
  showLabels?: boolean
  size?: "sm" | "md" | "lg"
  className?: string
}

const resultColors: Record<MatchResult, string> = {
  W: "bg-emerald-500 dark:bg-emerald-400",
  D: "bg-amber-500 dark:bg-amber-400",
  L: "bg-rose-500 dark:bg-rose-400",
}

const resultLabels: Record<MatchResult, string> = {
  W: "Win",
  D: "Draw",
  L: "Loss",
}

const sizeClasses = {
  sm: "w-2 h-2",
  md: "w-2.5 h-2.5",
  lg: "w-3 h-3",
}

export function FormDots({
  form,
  showLabels = false,
  size = "md",
  className,
}: FormDotsProps) {
  const description = form.map((r) => resultLabels[r]).join(", ")

  return (
    <div
      role="img"
      aria-label={`Recent form: ${description}`}
      className={cn("flex items-center gap-1.5", className)}
    >
      {form.map((result, index) => (
        <div key={index} className="flex flex-col items-center gap-0.5">
          <span
            className={cn(
              "rounded-full transition-transform hover:scale-110",
              sizeClasses[size],
              resultColors[result]
            )}
            title={resultLabels[result]}
          />
          {showLabels && (
            <span className="text-[10px] text-muted-foreground font-medium">
              {result}
            </span>
          )}
        </div>
      ))}
    </div>
  )
}
