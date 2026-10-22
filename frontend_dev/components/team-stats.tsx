"use client"

import { ArrowLeft, Calendar, MapPin, Trophy, Goal, Shield, Layers, TrendingUp, Activity, Target, ShieldOff } from 'lucide-react'
import { GlassButton } from "@/components/GlassButton"
import { useState } from "react"
import { StatCard } from "@/components/StatCard"
import { StreakChart } from "@/components/StreakChart"
import { WinLossPieChart } from "@/components/WinLossPieChart"

interface TeamStatsProps {
  teamName: string
  onBack: () => void
}

type FormResult = "W" | "D" | "L"

function TeamForm({ results }: { results: FormResult[] }) {
  const getResultColor = (result: FormResult) => {
    switch (result) {
      case "W":
        return "bg-emerald-500"
      case "D":
        return "bg-amber-500"
      case "L":
        return "bg-red-500"
    }
  }

  return (
    <div className="flex items-center gap-1.5 mt-2">
      {results.map((result, index) => (
        <div
          key={index}
          className={`${getResultColor(result)} w-8 h-8 rounded-md flex items-center justify-center text-white text-xs font-bold shadow-md`}
          title={result === "W" ? "Win" : result === "D" ? "Draw" : "Loss"}
        >
          {result}
        </div>
      ))}
    </div>
  )
}

export default function TeamStats({ teamName, onBack }: TeamStatsProps) {
  const [selectedStreakType, setSelectedStreakType] = useState<"all" | "result" | "goal" | "special">("all")

  const performanceData = [
    { date: "Match 1", value: 1 },
    { date: "Match 2", value: 3 },
    { date: "Match 3", value: 2 },
    { date: "Match 4", value: 4 },
    { date: "Match 5", value: 3 },
    { date: "Match 6", value: 5 },
    { date: "Match 7", value: 6 },
    { date: "Match 8", value: 8 },
  ]

  const homeTeamForm: FormResult[] = ["W", "W", "D", "W", "W"]
  const awayTeamForm: FormResult[] = ["L", "W", "D", "W", "L"]

  const teamStats = {
    wins: 18,
    draws: 4,
    losses: 3,
    totalGames: 25,
    goalsScored: 45,
    goalsConceded: 18,
  }

  const opponentStats = {
    wins: 12,
    draws: 7,
    losses: 6,
    totalGames: 25,
    goalsScored: 38,
    goalsConceded: 28,
  }

  const calculateRate = (value: number, total: number) => {
    return ((value / total) * 100).toFixed(1)
  }

  const mockStreaks = {
    result: [
      { type: "Win", team: 5, opponent: 3, leading: "team" },
      { type: "Win or Draw", team: 8, opponent: 7, leading: "team" },
      { type: "Halftime Win", team: 4, opponent: 4, leading: "tied" },
    ],
    goal: [
      { type: "Score 1 Goal", team: 12, opponent: 9, leading: "team" },
      { type: "Score 2 Goals", team: 6, opponent: 8, leading: "opponent" },
      { type: "Score 3 Goals", team: 3, opponent: 2, leading: "team" },
    ],
    special: [
      { type: "Clean Sheet", team: 4, opponent: 6, leading: "opponent" },
      { type: "Conceded 1 Goal", team: 5, opponent: 5, leading: "tied" },
      { type: "Win/Draw & Over 1.5", team: 7, opponent: 5, leading: "team" },
      { type: "Win/Draw & Under 4.5", team: 9, opponent: 8, leading: "team" },
    ],
  }

  const getFilteredStreaks = () => {
    if (selectedStreakType === "all") {
      return {
        result: mockStreaks.result,
        goal: mockStreaks.goal,
        special: mockStreaks.special,
      }
    } else if (selectedStreakType === "result") {
      return { result: mockStreaks.result, goal: [], special: [] }
    } else if (selectedStreakType === "goal") {
      return { result: [], goal: mockStreaks.goal, special: [] }
    } else {
      return { result: [], goal: [], special: mockStreaks.special }
    }
  }

  const filteredStreaks = getFilteredStreaks()
  const hasResultStreaks = filteredStreaks.result.length > 0
  const hasGoalStreaks = filteredStreaks.goal.length > 0
  const hasSpecialStreaks = filteredStreaks.special.length > 0

  const getCategoryDescription = (type: typeof selectedStreakType) => {
    switch (type) {
      case "all":
        return "Comprehensive comparison of all streak types between both teams"
      case "result":
        return "Match outcome streaks including wins, draws, and combinations"
      case "goal":
        return "Goal-scoring patterns and streaks for both teams"
      case "special":
        return "Defensive achievements and unique match circumstances"
      default:
        return ""
    }
  }

  return (
    <div
      className="min-h-screen p-6 font-sans text-foreground"
      style={{ background: "var(--page-background-gradient)" }}
    >
      <div className="max-w-6xl mx-auto">
        <div className="mb-8">
          <GlassButton variant="secondary" onClick={onBack} className="mb-6">
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Dashboard
          </GlassButton>

          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div>
              <h1 className="text-4xl font-bold text-foreground mb-2">{teamName}</h1>
              <p className="text-muted-foreground text-lg">Detailed performance analysis and streak history</p>
            </div>
            <div className="glass-pill px-4 py-2 text-sm font-medium text-emerald-500 bg-emerald-500/10 border-emerald-500/20">
              <span className="relative flex h-2 w-2 mr-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
              </span>
              Active Streak
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 mb-8">
          <div className="lg:col-span-2 glass-panel p-6">
            <h3 className="text-xl font-bold mb-6 flex items-center gap-2">
              <Activity className="h-5 w-5 text-emerald-500" />
              Performance Trend
            </h3>
            <StreakChart data={performanceData} />
          </div>

          <div className="glass-panel p-6">
            <h3 className="text-xl font-bold mb-6 flex items-center gap-2">
              <Trophy className="h-5 w-5 text-amber-500" />
              Season Overview
            </h3>
            <WinLossPieChart wins={teamStats.wins} draws={teamStats.draws} losses={teamStats.losses} />
          </div>
        </div>

        <div className="glass-panel p-6 mb-8">
          <div className="flex items-center gap-4 mb-6">
            <Calendar className="h-5 w-5 text-primary" />
            <h2 className="text-xl font-bold text-foreground">Fixture Analysis</h2>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-6">
            {/* Team Statistics */}
            <div>
              <h3 className="text-lg font-bold text-foreground mb-4">{teamName}</h3>
              <div className="space-y-4">
                <div className="flex items-center justify-between p-3 rounded-lg bg-emerald-500/5 border border-emerald-500/20">
                  <div className="flex items-center gap-2">
                    <Trophy className="h-4 w-4 text-emerald-400" />
                    <span className="text-sm font-medium text-muted-foreground">Win Rate</span>
                  </div>
                  <span className="text-lg font-bold text-emerald-400">{calculateRate(teamStats.wins, teamStats.totalGames)}%</span>
                </div>

                <div className="flex items-center justify-between p-3 rounded-lg bg-red-500/5 border border-red-500/20">
                  <div className="flex items-center gap-2">
                    <ShieldOff className="h-4 w-4 text-red-400" />
                    <span className="text-sm font-medium text-muted-foreground">Loss Rate</span>
                  </div>
                  <span className="text-lg font-bold text-red-400">{calculateRate(teamStats.losses, teamStats.totalGames)}%</span>
                </div>

                <div className="flex items-center justify-between p-3 rounded-lg bg-amber-500/5 border border-amber-500/20">
                  <div className="flex items-center gap-2">
                    <Activity className="h-4 w-4 text-amber-400" />
                    <span className="text-sm font-medium text-muted-foreground">Draw Rate</span>
                  </div>
                  <span className="text-lg font-bold text-amber-400">{calculateRate(teamStats.draws, teamStats.totalGames)}%</span>
                </div>

                <div className="flex items-center justify-between p-3 rounded-lg bg-blue-500/5 border border-blue-500/20">
                  <div className="flex items-center gap-2">
                    <Target className="h-4 w-4 text-blue-400" />
                    <span className="text-sm font-medium text-muted-foreground">Avg Goals Scored</span>
                  </div>
                  <span className="text-lg font-bold text-blue-400">{(teamStats.goalsScored / teamStats.totalGames).toFixed(2)}</span>
                </div>

                <div className="flex items-center justify-between p-3 rounded-lg bg-purple-500/5 border border-purple-500/20">
                  <div className="flex items-center gap-2">
                    <Shield className="h-4 w-4 text-purple-400" />
                    <span className="text-sm font-medium text-muted-foreground">Avg Goals Conceded</span>
                  </div>
                  <span className="text-lg font-bold text-purple-400">{(teamStats.goalsConceded / teamStats.totalGames).toFixed(2)}</span>
                </div>
              </div>

              <div className="mt-4">
                <span className="text-xs text-muted-foreground">Recent Form</span>
                <TeamForm results={homeTeamForm} />
              </div>
            </div>

            {/* Opponent Statistics */}
            <div>
              <h3 className="text-lg font-bold text-foreground mb-4">Newcastle United</h3>
              <div className="space-y-4">
                <div className="flex items-center justify-between p-3 rounded-lg bg-emerald-500/5 border border-emerald-500/20">
                  <div className="flex items-center gap-2">
                    <Trophy className="h-4 w-4 text-emerald-400" />
                    <span className="text-sm font-medium text-muted-foreground">Win Rate</span>
                  </div>
                  <span className="text-lg font-bold text-emerald-400">{calculateRate(opponentStats.wins, opponentStats.totalGames)}%</span>
                </div>

                <div className="flex items-center justify-between p-3 rounded-lg bg-red-500/5 border border-red-500/20">
                  <div className="flex items-center gap-2">
                    <ShieldOff className="h-4 w-4 text-red-400" />
                    <span className="text-sm font-medium text-muted-foreground">Loss Rate</span>
                  </div>
                  <span className="text-lg font-bold text-red-400">{calculateRate(opponentStats.losses, opponentStats.totalGames)}%</span>
                </div>

                <div className="flex items-center justify-between p-3 rounded-lg bg-amber-500/5 border border-amber-500/20">
                  <div className="flex items-center gap-2">
                    <Activity className="h-4 w-4 text-amber-400" />
                    <span className="text-sm font-medium text-muted-foreground">Draw Rate</span>
                  </div>
                  <span className="text-lg font-bold text-amber-400">{calculateRate(opponentStats.draws, opponentStats.totalGames)}%</span>
                </div>

                <div className="flex items-center justify-between p-3 rounded-lg bg-blue-500/5 border border-blue-500/20">
                  <div className="flex items-center gap-2">
                    <Target className="h-4 w-4 text-blue-400" />
                    <span className="text-sm font-medium text-muted-foreground">Avg Goals Scored</span>
                  </div>
                  <span className="text-lg font-bold text-blue-400">{(opponentStats.goalsScored / opponentStats.totalGames).toFixed(2)}</span>
                </div>

                <div className="flex items-center justify-between p-3 rounded-lg bg-purple-500/5 border border-purple-500/20">
                  <div className="flex items-center gap-2">
                    <Shield className="h-4 w-4 text-purple-400" />
                    <span className="text-sm font-medium text-muted-foreground">Avg Goals Conceded</span>
                  </div>
                  <span className="text-lg font-bold text-purple-400">{(opponentStats.goalsConceded / opponentStats.totalGames).toFixed(2)}</span>
                </div>
              </div>

              <div className="mt-4">
                <span className="text-xs text-muted-foreground">Recent Form</span>
                <TeamForm results={awayTeamForm} />
              </div>
            </div>
          </div>

          <div className="pt-6 border-t border-border">
            <div className="flex items-center justify-between text-sm">
              <div className="flex items-center gap-2">
                <Calendar className="h-4 w-4 text-muted-foreground" />
                <span className="text-muted-foreground">Match Date: <span className="font-semibold text-foreground">Jan 8, 2025 • 15:00</span></span>
              </div>
              <div className="flex items-center gap-2">
                <MapPin className="h-4 w-4 text-muted-foreground" />
                <span className="font-semibold text-foreground">Premier League</span>
              </div>
            </div>
          </div>
        </div>

        <div className="glass-panel p-6 mb-8">
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center gap-4">
              <Trophy className="h-5 w-5 text-primary" />
              <div>
                <h2 className="text-xl font-bold text-foreground">Streak Comparison</h2>
                <p className="text-sm text-muted-foreground mt-1">{getCategoryDescription(selectedStreakType)}</p>
              </div>
            </div>
          </div>

          <div
            className="mb-6 relative overflow-hidden rounded-2xl p-1"
            style={{
              background:
                "linear-gradient(135deg, rgba(139, 92, 246, 0.1) 0%, rgba(59, 130, 246, 0.1) 50%, rgba(16, 185, 129, 0.1) 100%)",
              backdropFilter: "blur(20px)",
              border: "1px solid rgba(255, 255, 255, 0.1)",
              boxShadow: "0 8px 32px 0 rgba(139, 92, 246, 0.15), inset 0 1px 0 0 rgba(255, 255, 255, 0.1)",
            }}
          >
            <div className="flex items-center justify-center gap-1 flex-wrap">
              <button
                onClick={() => setSelectedStreakType("all")}
                className={`flex items-center gap-2 px-4 py-2.5 rounded-xl transition-all duration-300 font-medium text-sm ${
                  selectedStreakType === "all"
                    ? "bg-gradient-to-r from-purple-500/60 to-blue-500/60 text-white shadow-lg shadow-purple-500/40 scale-105 border border-purple-400/50"
                    : "text-muted-foreground hover:text-foreground hover:bg-white/5"
                }`}
              >
                <Layers className="h-4 w-4" />
                <span>All Streaks</span>
              </button>

              <button
                onClick={() => setSelectedStreakType("result")}
                className={`flex items-center gap-2 px-4 py-2.5 rounded-xl transition-all duration-300 font-medium text-sm ${
                  selectedStreakType === "result"
                    ? "bg-gradient-to-r from-amber-500/60 to-orange-500/60 text-white shadow-lg shadow-amber-500/40 scale-105 border border-amber-400/50"
                    : "text-muted-foreground hover:text-foreground hover:bg-white/5"
                }`}
              >
                <Trophy className="h-4 w-4" />
                <span>Result Streaks</span>
              </button>

              <button
                onClick={() => setSelectedStreakType("goal")}
                className={`flex items-center gap-2 px-4 py-2.5 rounded-xl transition-all duration-300 font-medium text-sm ${
                  selectedStreakType === "goal"
                    ? "bg-gradient-to-r from-emerald-500/60 to-green-500/60 text-white shadow-lg shadow-emerald-500/40 scale-105 border border-emerald-400/50"
                    : "text-muted-foreground hover:text-foreground hover:bg-white/5"
                }`}
              >
                <Goal className="h-4 w-4" />
                <span>Goal Streaks</span>
              </button>

              <button
                onClick={() => setSelectedStreakType("special")}
                className={`flex items-center gap-2 px-4 py-2.5 rounded-xl transition-all duration-300 font-medium text-sm ${
                  selectedStreakType === "special"
                    ? "bg-gradient-to-r from-blue-500/60 to-cyan-500/60 text-white shadow-lg shadow-blue-500/40 scale-105 border border-blue-400/50"
                    : "text-muted-foreground hover:text-foreground hover:bg-white/5"
                }`}
              >
                <Shield className="h-4 w-4" />
                <span>Special Streaks</span>
              </button>
            </div>
          </div>

          {hasResultStreaks && (
            <div className="mb-8">
              <div className="flex items-center gap-2 mb-4 pb-2 border-b border-border">
                <Trophy className="h-5 w-5 text-amber-500" />
                <h3 className="text-lg font-bold text-foreground">Result Streaks</h3>
              </div>
              <div className="space-y-3">
                <div className="grid grid-cols-3 gap-4 text-sm font-semibold text-muted-foreground pb-2 border-b border-border/50">
                  <div className="text-left">{teamName}</div>
                  <div className="text-center">Streak Type</div>
                  <div className="text-right">Newcastle United</div>
                </div>
                {filteredStreaks.result.map((streak, index) => (
                  <div
                    key={index}
                    className="grid grid-cols-3 gap-4 items-center p-3 rounded-lg hover:bg-white/5 transition-colors"
                  >
                    <div className="flex items-center gap-2">
                      <span
                        className={`text-2xl font-bold ${streak.leading === "team" ? "text-emerald-400" : "text-foreground"}`}
                      >
                        {streak.team}
                      </span>
                      {streak.leading === "team" && <Trophy className="h-4 w-4 text-amber-400" />}
                    </div>
                    <div className="text-center">
                      <span className="text-sm font-medium text-foreground">{streak.type}</span>
                    </div>
                    <div className="flex items-center justify-end gap-2">
                      {streak.leading === "opponent" && <Trophy className="h-4 w-4 text-amber-400" />}
                      <span
                        className={`text-2xl font-bold ${streak.leading === "opponent" ? "text-emerald-400" : "text-foreground"}`}
                      >
                        {streak.opponent}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {hasGoalStreaks && (
            <div className="mb-8">
              <div className="flex items-center gap-2 mb-4 pb-2 border-b border-border">
                <Goal className="h-5 w-5 text-emerald-500" />
                <h3 className="text-lg font-bold text-foreground">Goal Streaks</h3>
              </div>
              <div className="space-y-3">
                <div className="grid grid-cols-3 gap-4 text-sm font-semibold text-muted-foreground pb-2 border-b border-border/50">
                  <div className="text-left">{teamName}</div>
                  <div className="text-center">Streak Type</div>
                  <div className="text-right">Newcastle United</div>
                </div>
                {filteredStreaks.goal.map((streak, index) => (
                  <div
                    key={index}
                    className="grid grid-cols-3 gap-4 items-center p-3 rounded-lg hover:bg-white/5 transition-colors"
                  >
                    <div className="flex items-center gap-2">
                      <span
                        className={`text-2xl font-bold ${streak.leading === "team" ? "text-emerald-400" : "text-foreground"}`}
                      >
                        {streak.team}
                      </span>
                      {streak.leading === "team" && <Trophy className="h-4 w-4 text-amber-400" />}
                    </div>
                    <div className="text-center">
                      <span className="text-sm font-medium text-foreground">{streak.type}</span>
                    </div>
                    <div className="flex items-center justify-end gap-2">
                      {streak.leading === "opponent" && <Trophy className="h-4 w-4 text-amber-400" />}
                      <span
                        className={`text-2xl font-bold ${streak.leading === "opponent" ? "text-emerald-400" : "text-foreground"}`}
                      >
                        {streak.opponent}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {hasSpecialStreaks && (
            <div>
              <div className="flex items-center gap-2 mb-4 pb-2 border-b border-border">
                <Shield className="h-5 w-5 text-blue-500" />
                <h3 className="text-lg font-bold text-foreground">Special Streaks</h3>
              </div>
              <div className="space-y-3">
                <div className="grid grid-cols-3 gap-4 text-sm font-semibold text-muted-foreground pb-2 border-b border-border/50">
                  <div className="text-left">{teamName}</div>
                  <div className="text-center">Streak Type</div>
                  <div className="text-right">Newcastle United</div>
                </div>
                {filteredStreaks.special.map((streak, index) => (
                  <div
                    key={index}
                    className="grid grid-cols-3 gap-4 items-center p-3 rounded-lg hover:bg-white/5 transition-colors"
                  >
                    <div className="flex items-center gap-2">
                      <span
                        className={`text-2xl font-bold ${streak.leading === "team" ? "text-emerald-400" : "text-foreground"}`}
                      >
                        {streak.team}
                      </span>
                      {streak.leading === "team" && <Trophy className="h-4 w-4 text-amber-400" />}
                    </div>
                    <div className="text-center">
                      <span className="text-sm font-medium text-foreground">{streak.type}</span>
                    </div>
                    <div className="flex items-center justify-end gap-2">
                      {streak.leading === "opponent" && <Trophy className="h-4 w-4 text-amber-400" />}
                      <span
                        className={`text-2xl font-bold ${streak.leading === "opponent" ? "text-emerald-400" : "text-foreground"}`}
                      >
                        {streak.opponent}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div className="mt-6 pt-6 border-t border-border">
            <div className="flex items-center gap-6 text-sm">
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full bg-emerald-400"></div>
                <span className="text-muted-foreground">Leading Streak</span>
              </div>
              <div className="flex items-center gap-2">
                <Trophy className="h-4 w-4 text-amber-400" />
                <span className="text-muted-foreground">Best Performer</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full bg-foreground/30"></div>
                <span className="text-muted-foreground">Equal Streak</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
