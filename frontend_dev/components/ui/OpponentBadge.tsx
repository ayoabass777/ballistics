"use client"

import { cn } from "@/lib/utils"

interface OpponentBadgeProps {
  name: string
  logoUrl?: string
  className?: string
}

export function OpponentBadge({ name, logoUrl, className }: OpponentBadgeProps) {
  return (
    <div className={cn("flex items-center gap-2", className)}>
      {logoUrl ? (
        <img
          src={logoUrl}
          alt={name}
          className="w-5 h-5 object-contain"
        />
      ) : (
        <div className="w-5 h-5 rounded-full bg-muted flex items-center justify-center">
          <span className="text-[10px] font-bold text-muted-foreground">
            {name.charAt(0)}
          </span>
        </div>
      )}
      <span className="text-sm">
        <span className="text-muted-foreground">vs </span>
        <span className="font-medium text-foreground">{name}</span>
      </span>
    </div>
  )
}
