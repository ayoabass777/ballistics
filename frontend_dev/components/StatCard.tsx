"use client"

import { type LucideIcon } from 'lucide-react'
import { AnimatedCounter } from "@/components/AnimatedCounter"

interface StatCardProps {
  title: string
  value: number
  suffix?: string
  icon: LucideIcon
  gradient?: "emerald" | "amber" | "blue" | "red"
  trend?: {
    value: number
    isPositive: boolean
  }
}

export function StatCard({ title, value, suffix = "", icon: Icon, gradient = "emerald", trend }: StatCardProps) {
  const gradientClasses = {
    emerald: "from-emerald-500/20 to-teal-500/20 border-emerald-500/30",
    amber: "from-amber-500/20 to-orange-500/20 border-amber-500/30",
    blue: "from-blue-500/20 to-cyan-500/20 border-blue-500/30",
    red: "from-red-500/20 to-rose-500/20 border-red-500/30",
  }

  const iconColors = {
    emerald: "text-emerald-400",
    amber: "text-amber-400",
    blue: "text-blue-400",
    red: "text-red-400",
  }

  return (
    <div
      className={`glass-panel p-6 hover-lift animate-scale-in bg-gradient-to-br ${gradientClasses[gradient]}`}
    >
      <div className="flex items-start justify-between mb-4">
        <div className="p-3 rounded-xl glass-button">
          <Icon className={`h-6 w-6 ${iconColors[gradient]}`} />
        </div>
        {trend && (
          <div className={`flex items-center gap-1 text-sm font-medium ${trend.isPositive ? "text-emerald-400" : "text-red-400"}`}>
            <span>{trend.isPositive ? "+" : ""}{trend.value}%</span>
          </div>
        )}
      </div>
      <div className="space-y-1">
        <p className="text-sm font-medium text-muted-foreground">{title}</p>
        <p className="text-3xl font-bold text-foreground">
          <AnimatedCounter end={value} suffix={suffix} />
        </p>
      </div>
    </div>
  )
}
