import React, { useState, useEffect } from 'react'
import { Head, Link } from '@inertiajs/react'
import Chart from 'react-apexcharts'
import Header from '@/components/Header'

interface MediaItem {
  url: string
  size_variants?: string[]
  width?: number
  height?: number
  external_id?: string
  checksum?: string
}

interface Game {
  id: number
  name: string
  canonical_name: string
  rating: number
  release_date: string
  description: string
  synopsis: string
  developer: string
  publisher: string
  platforms: string[]
  genres: string[]
  media: {
    cover: MediaItem | null
    screenshots: MediaItem[]
    artworks: MediaItem[]
    trailers: any[]
    hero_url?: string
    cover_url_high_res: string
    cover_url_mobile: string
    summary: {
      images: {
        has_cover: boolean
        has_screenshots: boolean
        has_artworks: boolean
        total_count: number
        hero_url?: string
      }
      videos: {
        has_trailers: boolean
        total_count: number
      }
    }
  }
}

interface PriceData {
  currency: string
  countries: Array<{
    country: string
    min_price: number
    max_price: number
    avg_price: number
  }>
}

interface AvailabilityData {
  country: string
  country_code: string
  retailer_count: number
  currency_count: number
  availability_score: number
}

interface Props {
  game: Game
  priceData: PriceData[]
  availabilityData: AvailabilityData[]
  meta: {
    query_time: number
    cached: {
      game: boolean
      prices: boolean
      availability: boolean
    }
  }
}

export default function DashboardShow({ game, priceData, availabilityData, meta }: Props) {
  const [activeView, setActiveView] = useState('chart') // 'chart' or 'cover'
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [currentMediaIndex, setCurrentMediaIndex] = useState(0)
  const [isChartLoading, setIsChartLoading] = useState(true)
  const [isVideoPlaying, setIsVideoPlaying] = useState(false)

  // Background image priority: Hero (Art/Promo) > Cover
  const backgroundImage = game.media.hero_url || game.media.cover_url_high_res || game.media.cover_url_mobile

  // Get all media items for carousel
  const allMedia = [...game.media.screenshots, ...game.media.artworks]

  useEffect(() => {
    // Simulate chart loading time (1-3 seconds)
    const timer = setTimeout(() => {
      setIsChartLoading(false)
    }, Math.random() * 2000 + 1000)

    return () => clearTimeout(timer)
  }, [])

  // Auto-play video functionality
  useEffect(() => {
    if (activeView === 'cover' && game.media.trailers.length > 0) {
      const timer = setTimeout(() => {
        setIsVideoPlaying(true)
      }, 1000) // Auto-play after 1 second

      return () => clearTimeout(timer)
    }
  }, [activeView, game.media.trailers])

  // Price comparison chart configuration
  const priceChartOptions = {
    chart: {
      type: 'bar' as const,
      background: 'transparent',
      toolbar: { show: false },
    },
    theme: {
      mode: 'dark' as const,
    },
    plotOptions: {
      bar: {
        horizontal: true,
        dataLabels: { position: 'top' as const },
      },
    },
    dataLabels: {
      enabled: true,
      formatter: (val: number) => val.toFixed(2),
      style: { colors: ['#fff'] },
    },
    xaxis: {
      categories: priceData.map(pd => pd.currency),
      labels: { style: { colors: '#fff' } },
    },
    yaxis: {
      labels: { style: { colors: '#fff' } },
    },
    grid: {
      borderColor: '#374151',
    },
    colors: ['#3B82F6', '#EF4444', '#10B981'],
  }

  const priceChartSeries = [
    {
      name: 'Min Price',
      data: priceData.map(pd => pd.countries[0]?.min_price || 0),
    },
    {
      name: 'Max Price',
      data: priceData.map(pd => pd.countries[0]?.max_price || 0),
    },
    {
      name: 'Avg Price',
      data: priceData.map(pd => pd.countries[0]?.avg_price || 0),
    },
  ]

  // Availability chart configuration
  const availabilityChartOptions = {
    chart: {
      type: 'donut' as const,
      background: 'transparent',
    },
    theme: {
      mode: 'dark' as const,
    },
    labels: availabilityData.map(ad => ad.country),
    colors: ['#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#06B6D4'],
    legend: {
      labels: { colors: '#fff' },
    },
    plotOptions: {
      pie: {
        donut: {
          size: '70%',
          labels: {
            show: true,
            total: {
              show: true,
              label: 'Countries',
              color: '#fff',
            },
          },
        },
      },
    },
  }

  const availabilityChartSeries = availabilityData.map(ad => ad.availability_score)

  const openModal = (index: number = 0) => {
    setCurrentMediaIndex(index)
    setIsModalOpen(true)
  }

  const nextMedia = () => {
    setCurrentMediaIndex((prev) => (prev + 1) % allMedia.length)
  }

  const prevMedia = () => {
    setCurrentMediaIndex((prev) => (prev - 1 + allMedia.length) % allMedia.length)
  }

  return (
    <>
      <Head title={`${game.name} - Dashboard`} />

      <div className="min-h-screen bg-black">
        <Header />

        <div
          className="min-h-[calc(100vh-4rem)] bg-cover bg-center bg-fixed relative"
          style={{
            backgroundImage: backgroundImage ? `url(${backgroundImage})` : 'none',
            backgroundColor: '#000000',
          }}
        >
          {/* Background Overlay - Darken for text but NO BLUR */}
          <div className="absolute inset-0 bg-black/40"></div>

          {/* Mobile Background Optimization - Removed Blur */}
          <div
            className="absolute inset-0 md:hidden bg-cover bg-center"
            style={{
              backgroundImage: game.media.cover_url_mobile ? `url(${game.media.cover_url_mobile})` : 'none',
            }}
          />
          <div className="absolute inset-0 md:hidden bg-black/60" />

          {/* Content */}
          <div className="relative z-10">
            {/* Sub-header */}
            <div className="bg-black/80 backdrop-blur-sm border-b border-white/10">
              <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div className="flex items-center h-16">
                  <Link
                    href="/dashboard"
                    className="text-white hover:text-blue-300 transition-colors mr-4"
                  >
                    ← Back to Dashboard
                  </Link>
                  <h1 className="text-xl font-bold text-white truncate">{game.name}</h1>

                  {/* Performance Stats */}
                  <div className="ml-auto text-sm text-gray-400">
                    Loaded in {(meta.query_time * 1000).toFixed(2)}ms
                  </div>
                </div>
              </div>
            </div>

            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">

                {/* Main Display Panel - Left Column */}
                <div className="lg:col-span-2 space-y-6">
                  {/* View Toggle Buttons */}
                  <div className="flex gap-4">
                    <button
                      onClick={() => setActiveView('chart')}
                      className={`px-6 py-3 rounded-xl font-medium transition-all ${
                        activeView === 'chart'
                          ? 'bg-blue-600 text-white shadow-lg'
                          : 'bg-white/10 text-gray-300 hover:bg-white/20'
                      }`}
                    >
                      Charts
                    </button>
                    <button
                      onClick={() => setActiveView('cover')}
                      className={`px-6 py-3 rounded-xl font-medium transition-all ${
                        activeView === 'cover'
                          ? 'bg-blue-600 text-white shadow-lg'
                          : 'bg-white/10 text-gray-300 hover:bg-white/20'
                      }`}
                    >
                      Cover/Trailer
                    </button>
                  </div>

                  {/* Main Display */}
                  <div className="bg-black/60 rounded-xl border border-white/20 overflow-hidden min-h-[400px]">
                    {activeView === 'chart' ? (
                      <div className="p-6">
                        {/* Price Analysis Chart */}
                        <div className="mb-8">
                          <h3 className="text-xl font-bold text-white mb-4">Price Analysis</h3>
                          {isChartLoading ? (
                            <div className="flex items-center justify-center h-64">
                              <div className="text-white">Loading chart...</div>
                            </div>
                          ) : (
                            <Chart
                              options={priceChartOptions}
                              series={priceChartSeries}
                              type="bar"
                              height="300"
                            />
                          )}
                        </div>

                        {/* Availability Chart */}
                        <div>
                          <h3 className="text-xl font-bold text-white mb-4">Availability by Region</h3>
                          {isChartLoading ? (
                            <div className="flex items-center justify-center h-64">
                              <div className="text-white">Loading chart...</div>
                            </div>
                          ) : (
                            <Chart
                              options={availabilityChartOptions}
                              series={availabilityChartSeries}
                              type="donut"
                              height="300"
                            />
                          )}
                        </div>
                      </div>
                    ) : (
                      <div className="relative">
                        {/* Auto-playing Cover/Trailer */}
                        {game.media.trailers.length > 0 && isVideoPlaying ? (
                          <div className="relative aspect-video">
                            <video
                              autoPlay
                              muted
                              loop
                              className="w-full h-full object-cover"
                              poster={backgroundImage}
                            >
                              <source src={game.media.trailers[0]?.url} type="video/mp4" />
                              Your browser does not support the video tag.
                            </video>
                            <button
                              onClick={() => setIsVideoPlaying(false)}
                              className="absolute top-4 right-4 bg-black/50 text-white p-2 rounded-full hover:bg-black/70 transition-colors"
                            >
                              ⏸️
                            </button>
                          </div>
                        ) : (
                          <div className="relative aspect-video">
                            <img
                              src={backgroundImage}
                              alt={game.name}
                              className="w-full h-full object-cover"
                            />
                            {game.media.trailers.length > 0 && (
                              <button
                                onClick={() => setIsVideoPlaying(true)}
                                className="absolute inset-0 flex items-center justify-center bg-black/30 hover:bg-black/50 transition-colors group"
                              >
                                <div className="bg-white/90 text-black p-4 rounded-full group-hover:scale-110 transition-transform">
                                  ▶️
                                </div>
                              </button>
                            )}
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  {/* Image Carousel */}
                  {allMedia.length > 0 && (
                    <div className="bg-black/60 rounded-xl border border-white/20 p-6">
                      <h3 className="text-xl font-bold text-white mb-4">Screenshots & Artwork</h3>
                      <div className="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-6 gap-4">
                        {allMedia.slice(0, 12).map((media, index) => (
                          <button
                            key={index}
                            onClick={() => openModal(index)}
                            className="relative aspect-video rounded-lg overflow-hidden group hover:scale-105 transition-transform"
                          >
                            <img
                              src={media.url}
                              alt={`Screenshot ${index + 1}`}
                              className="w-full h-full object-cover"
                              loading="lazy"
                            />
                            <div className="absolute inset-0 bg-black/0 group-hover:bg-black/30 transition-colors flex items-center justify-center">
                              <span className="text-white opacity-0 group-hover:opacity-100 transition-opacity">
                                🔍
                              </span>
                            </div>
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                </div>

                {/* Game Info - Right Column */}
                <div className="space-y-6">
                  {/* Game Details */}
                  <div className="bg-black/60 rounded-xl border border-white/20 p-6">
                    <h2 className="text-2xl font-bold text-white mb-4">{game.name}</h2>

                    {/* Rating */}
                    {game.rating && (
                      <div className="flex items-center mb-4">
                        <span className="text-yellow-400 text-xl mr-2">⭐</span>
                        <span className="text-white text-lg font-semibold">{game.rating.toFixed(1)}</span>
                        <span className="text-gray-400 text-sm ml-2">/10</span>
                      </div>
                    )}

                    {/* Release Date */}
                    {game.release_date && (
                      <div className="mb-4">
                        <span className="text-gray-400">Release Date: </span>
                        <span className="text-white">{new Date(game.release_date).toLocaleDateString()}</span>
                      </div>
                    )}

                    {/* Developer & Publisher */}
                    {game.developer && (
                      <div className="mb-2">
                        <span className="text-gray-400">Developer: </span>
                        <span className="text-white">{game.developer}</span>
                      </div>
                    )}
                    {game.publisher && (
                      <div className="mb-4">
                        <span className="text-gray-400">Publisher: </span>
                        <span className="text-white">{game.publisher}</span>
                      </div>
                    )}

                    {/* Genres */}
                    {game.genres && game.genres.length > 0 && (
                      <div className="mb-4">
                        <span className="text-gray-400 block mb-2">Genres:</span>
                        <div className="flex flex-wrap gap-2">
                          {game.genres.map((genre, index) => (
                            <span key={index} className="bg-blue-600/30 text-blue-300 px-3 py-1 rounded-full text-sm">
                              {genre}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Platforms */}
                    {game.platforms && game.platforms.length > 0 && (
                      <div className="mb-4">
                        <span className="text-gray-400 block mb-2">Platforms:</span>
                        <div className="flex flex-wrap gap-2">
                          {game.platforms.map((platform, index) => (
                            <span key={index} className="bg-gray-600/30 text-gray-300 px-3 py-1 rounded-full text-sm">
                              {platform}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Description */}
                    {game.description && (
                      <div>
                        <h3 className="text-lg font-semibold text-white mb-2">Description</h3>
                        <p className="text-gray-300 leading-relaxed">{game.description}</p>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Full Screen Modal for Image Carousel */}
        {isModalOpen && (
          <div className="fixed inset-0 bg-black/90 z-50 flex items-center justify-center">
            <div className="relative max-w-7xl max-h-screen p-4">
              {/* Close Button */}
              <button
                onClick={() => setIsModalOpen(false)}
                className="absolute top-4 right-4 z-10 bg-black/50 text-white p-3 rounded-full hover:bg-black/70 transition-colors"
              >
                ✕
              </button>

              {/* Navigation Buttons */}
              {allMedia.length > 1 && (
                <>
                  <button
                    onClick={prevMedia}
                    className="absolute left-4 top-1/2 -translate-y-1/2 z-10 bg-black/50 text-white p-3 rounded-full hover:bg-black/70 transition-colors"
                  >
                    ←
                  </button>
                  <button
                    onClick={nextMedia}
                    className="absolute right-4 top-1/2 -translate-y-1/2 z-10 bg-black/50 text-white p-3 rounded-full hover:bg-black/70 transition-colors"
                  >
                    →
                  </button>
                </>
              )}

              {/* Current Image */}
              {allMedia[currentMediaIndex] && (
                <img
                  src={allMedia[currentMediaIndex].url}
                  alt={`Media ${currentMediaIndex + 1}`}
                  className="max-w-full max-h-full object-contain rounded-lg"
                />
              )}

              {/* Image Counter */}
              <div className="absolute bottom-4 left-1/2 -translate-x-1/2 bg-black/50 text-white px-4 py-2 rounded-full">
                {currentMediaIndex + 1} / {allMedia.length}
              </div>
            </div>

            {/* Thumbnail Strip */}
            <div className="absolute bottom-4 left-1/2 -translate-x-1/2 max-w-7xl">
              <div className="flex gap-2 px-4 py-2 bg-black/50 rounded-lg max-w-full overflow-x-auto">
                {allMedia.map((media, index) => (
                  <button
                    key={index}
                    onClick={() => setCurrentMediaIndex(index)}
                    className={`flex-shrink-0 w-16 h-12 rounded overflow-hidden border-2 transition-all ${
                      index === currentMediaIndex ? 'border-blue-400' : 'border-transparent hover:border-gray-400'
                    }`}
                  >
                    <img
                      src={media.url}
                      alt={`Thumbnail ${index + 1}`}
                      className="w-full h-full object-cover"
                    />
                  </button>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </>
  )
}