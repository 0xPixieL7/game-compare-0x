import React, { useState, useEffect, useMemo } from 'react'
import { Head, Link, usePage } from '@inertiajs/react'
import Header from '@/components/Header'
import { GameCard } from '@/components/GameCard'
import EndlessCarousel from '@/components/EndlessCarousel'
import { useUserPreferences } from '@/Utils/userPreferences'

interface Game {
  id: number
  name: string
  canonical_name: string
  rating: number
  release_date: string
  media: {
    cover_url: string
    cover_url_thumb: string
    screenshots: Array<{ url: string; width: number; height: number }>
    trailers: Array<{ url?: string; thumbnail?: string; name?: string; video_id?: string }>
  }
}

interface CarouselRow {
  id: string
  title: string
  type: 'user_list' | 'recent' | 'genre' | 'top_rated' | 'new_releases'
  games: Game[]
  genre?: string
  description: string
}

interface Props {
  carouselRows: CarouselRow[]
  searchResults: Game[]
  search: string
  meta: {
    total_rows: number
    query_time: number
  }
}

export default function DashboardIndex({ carouselRows, searchResults, search, meta }: Props) {
  const { props } = usePage()
  const isAuthenticated = !!(props.auth as any)?.user
  const [searchTerm, setSearchTerm] = useState(search)
  const [isLoading, setIsLoading] = useState(false)
  const [populatedRows, setPopulatedRows] = useState<CarouselRow[]>([])

  // User preferences hook
  const preferences = useUserPreferences(isAuthenticated)

  // Populate user preference and recent rows on the frontend
  useEffect(() => {
    const processedRows = carouselRows.map(row => {
      if (row.type === 'user_list') {
        // Get games from user's favorite list
        const userLists = preferences.getLists()
        const favoritesList = userLists.find(list => list.id === 'favorites')
        const wishList = userLists.find(list => list.id === 'wishlist')

        // Combine favorite and wishlist games (remove duplicates)
        const allUserGameIds = new Set([
          ...(favoritesList?.games || []),
          ...(wishList?.games || [])
        ])

        // Find matching games from all carousel rows
        const userGames: Game[] = []
        carouselRows.forEach(otherRow => {
          if (otherRow.type !== 'user_list' && otherRow.type !== 'recent') {
            otherRow.games.forEach(game => {
              if (allUserGameIds.has(game.id) && !userGames.find(g => g.id === game.id)) {
                userGames.push(game)
              }
            })
          }
        })

        return { ...row, games: userGames }
      }

      if (row.type === 'recent') {
        // Get recently viewed games
        const recentGameIds = preferences.getRecentlyViewed()

        // Find matching games from all carousel rows
        const recentGames: Game[] = []
        recentGameIds.forEach(gameId => {
          carouselRows.forEach(otherRow => {
            if (otherRow.type !== 'user_list' && otherRow.type !== 'recent') {
              const game = otherRow.games.find(g => g.id === gameId)
              if (game && !recentGames.find(g => g.id === game.id)) {
                recentGames.push(game)
              }
            }
          })
        })

        return { ...row, games: recentGames }
      }

      return row
    })

    setPopulatedRows(processedRows)
  }, [carouselRows, preferences])

  // Track when user views a game
  const trackGameView = (gameId: number) => {
    preferences.addToRecentlyViewed(gameId)
  }

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)
    window.location.href = `/dashboard?search=${encodeURIComponent(searchTerm)}`
  }

  // Filter out rows with no games
  const validRows = populatedRows.filter(row => row.games.length > 0)

  return (
    <>
      <Head title="Game Dashboard" />

      <div className="min-h-screen bg-black">
        <Header />

        {/* Sub-header with search */}
        <div className="bg-black/80 backdrop-blur-sm border-b border-white/10">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex items-center justify-between h-16">
              <div className="flex items-center">
                <h1 className="text-2xl font-bold text-white">Game Dashboard</h1>
                <div className="ml-4 px-3 py-1 bg-white/10 rounded-full text-xs text-gray-300">
                  {isAuthenticated ? 'Authenticated' : 'Guest Session (30min)'}
                </div>
              </div>

              {/* Search */}
              <form onSubmit={handleSearch} className="flex items-center space-x-4">
                <div className="relative">
                  <input
                    type="text"
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    placeholder="Search games..."
                    className="w-64 px-4 py-2 bg-white/10 border border-white/20 rounded-lg text-white placeholder-gray-300 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  />
                  <button
                    type="submit"
                    disabled={isLoading}
                    className="absolute right-2 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-white"
                  >
                    🔍
                  </button>
                </div>
              </form>
            </div>
          </div>
        </div>

        {/* Performance Stats */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="bg-black/20 backdrop-blur-md rounded-lg p-4 border border-white/10">
            <div className="flex items-center justify-between text-sm text-gray-300">
              <span>{meta.total_rows} carousel rows loaded</span>
              <span>Query time: {(meta.query_time * 1000).toFixed(2)}ms</span>
            </div>
          </div>
        </div>

        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-12">
          {/* Search Results */}
          {search && searchResults.length > 0 && (
            <div className="mb-12">
              <h2 className="text-2xl font-semibold text-white mb-6">
                Search Results for "{search}"
              </h2>
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-7 gap-6">
                {searchResults.map((game) => (
                  <div key={game.id} onClick={() => trackGameView(game.id)}>
                    <GameCard 
                      game={game} 
                      className="aspect-[2/3]"
                    />
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Search state message */}
          {search && searchResults.length === 0 && (
            <div className="text-center py-12">
              <div className="text-gray-400 text-lg">
                No games found for "{search}"
              </div>
            </div>
          )}

          {/* Carousel Rows */}
          {!search && (
            <div className="space-y-16">
              {validRows.map((row) => {
                // Convert games to the format expected by EndlessCarousel
                const carouselGames = row.games.map(game => ({
                  ...game,
                  media: {
                    ...game.media,
                    cover_url: game.media.cover_url_thumb || game.media.cover_url,
                    cover_url_thumb: game.media.cover_url_thumb || game.media.cover_url,
                    cover: { url: game.media.cover_url_thumb || game.media.cover_url, width: 0, height: 0 }
                  }
                })) as any[]

                return (
                  <div
                    key={row.id}
                    onClick={(e) => {
                      // Track clicks on games in carousels
                      const target = e.target as HTMLElement
                      const gameLink = target.closest('a')
                      if (gameLink) {
                        const href = gameLink.getAttribute('href')
                        const match = href?.match(/\/dashboard\/(\d+)/)
                        if (match) {
                          trackGameView(parseInt(match[1]))
                        }
                      }
                    }}
                  >
                    <EndlessCarousel
                      title={row.title}
                      games={carouselGames}
                      className="mb-8"
                    />
                  </div>
                )
              })}
            </div>
          )}

          {/* Empty state for no rows */}
          {!search && validRows.length === 0 && (
            <div className="text-center py-12">
              <div className="text-gray-400 text-lg">
                No games available in any category
              </div>
            </div>
          )}
        </main>
      </div>
    </>
  )
}