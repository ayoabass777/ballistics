"use client"

import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts"

interface WinLossPieChartProps {
  wins: number
  draws: number
  losses: number
}

export function WinLossPieChart({ wins, draws, losses }: WinLossPieChartProps) {
  const data = [
    { name: "Wins", value: wins, color: "#10b981" }, // Emerald
    { name: "Draws", value: draws, color: "#94a3b8" }, // Slate
    { name: "Losses", value: losses, color: "#ef4444" }, // Red
  ]

  return (
    <div className="h-[250px] w-full animate-scale-in">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={60}
            outerRadius={80}
            paddingAngle={5}
            dataKey="value"
            stroke="none"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.color} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{
              backgroundColor: "rgba(var(--glass-base-rgb), 0.8)",
              backdropFilter: "blur(12px)",
              border: "1px solid rgba(var(--glass-border-rgb), 0.2)",
              borderRadius: "12px",
              boxShadow: "0 4px 16px rgba(0,0,0,0.1)",
              color: "var(--foreground)",
            }}
          />
        </PieChart>
      </ResponsiveContainer>

      <div className="flex justify-center gap-6 mt-4">
        {data.map((entry) => (
          <div key={entry.name} className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full" style={{ backgroundColor: entry.color }} />
            <span className="text-sm font-medium text-muted-foreground">
              {entry.name} ({entry.value})
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}
