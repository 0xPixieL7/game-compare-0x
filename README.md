# Game Compare: Global Pricing & Discovery Engine

![CI Status](https://github.com/lowkey/game-compare/actions/workflows/ci.yml/badge.svg)
![PHP Version](https://img.shields.io/badge/PHP-8.3-777BB4.svg)
![Laravel](https://img.shields.io/badge/Laravel-11-FF2D20.svg)
![React](https://img.shields.io/badge/React-19-61DAFB.svg)
![Inertia.js](https://img.shields.io/badge/Inertia.js-2.0-9553E9.svg)

**Game Compare** is a high-performance, full-stack application designed to aggregate, normalize, and compare video game pricing data across global markets.

Unlike standard game databases, this project focuses on **financial transparency** in the gaming market, offering real-time price disparity analysis (Arbitrage) and Bitcoin-based value comparisons. It ingests massive datasets from providers like Steam, PlayStation, and IGDB, processing them through a resilient background queuing system.

---

## 🚀 Key Features

- **Multi-Source Data Ingestion**: Robust pipelines to fetch and normalize data from **Steam**, **PlayStation Store**, **Xbox**, and **IGDB**.
- **Global Price Analysis**:
    - Real-time conversion of regional prices to **Bitcoin (BTC)** and Fiat.
    - **Arbitrage Detection**: Visualizing price disparities between regions (e.g., Turkey vs. US vs. Japan).
    - Interactive `PriceDisparityChart` for analyzing market spread.
- **Cinematic UI/UX**:
    - Built with **React 19** & **Inertia.js 2.0** for a seamless SPA experience.
    - Dynamic theming that adapts to the dominant colors of game artwork.
    - Rich media galleries with lightbox support and video playback.
- **High-Performance Architecture**:
    - Uses **Materialized Views** for sub-millisecond sorting and filtering of millions of records.
    - **Redis-powered Queues** (Laravel Horizon) for background data processing and logic enrichment.
    - **Service-Oriented Design** decoupling business logic from controllers.

---

## 🛠️ Technical Stack

### Backend (Laravel)

- **Architecture**: Service-Layer Pattern, Command Bus for ingestion.
- **Database**: PostgreSQL (Production) / SQLite (Testing).
    - _Highlights_: JSONB columns for flexible attribute storage, Materialized Views for reporting.
- **Caching**: Redis & Octane (Swoole/FrankenPHP ready).
- **Testing**: PestPHP for Unit/Feature testing with strict type coverage.

### Frontend (React & Inertia)

- **State Management**: Inertia.js (Server-driven state).
- **Styling**: Tailwind CSS v4, Lucide React Icons.
- **Visuals**: Framer Motion for entrance animations, ApexCharts for data visualization.
- **Tooling**: Vite, TypeScript, Prettier/ESLint.

### DevOps & Infrastructure

- **CI/CD**: GitHub Actions pipeline for automated testing and linting.
- **Containerization**: Docker (Laravel Sail) for consistent dev environments.

---

## 🏗️ System Architecture

This project follows a strict **ETL (Extract, Transform, Load)** pattern for game data:

1.  **Ingestion**: Scheduled commands (`steam:import-apps`, `psn:ingest`) fetch raw JSON payloads.
2.  **Normalization**: The `GameDataAggregatorService` normalizes diverse schemas into a unified `VideoGame` model.
3.  **Enrichment**: Background jobs enrich base records with metadata, media (Spatie Media Library), and Hype/Popscores.
4.  **Presentation**: Data is served via Inertia props, minimizing API round-trips and ensuring SEO capabilities.

---

## ⚡ Getting Started

### Prerequisites

- Docker & Docker Compose (Recommended)
- _Or:_ PHP 8.3+, Node.js 20+, PostgreSQL

### Installation

1.  **Clone the repository**

    ```bash
    git clone https://github.com/yourusername/game-compare.git
    cd game-compare
    ```

2.  **Install Backend Dependencies**

    ```bash
    composer install
    ```

3.  **Setup Environment**

    ```bash
    cp .env.example .env
    php artisan key:generate
    ```

4.  **Start with Sail (Docker)**

    ```bash
    ./vendor/bin/sail up -d
    ./vendor/bin/sail artisan migrate --seed
    ```

5.  **Install Frontend Dependencies**
    ```bash
    npm install
    npm run dev
    ```

### Running Tests

The project uses a complete CI pipeline. You can run the test suite locally using:

```bash
# Run PHP Tests (Pest)
php artisan test

# Run Code Style Checks
./vendor/bin/pint --test
npm run lint
```

---

## 📸 Screenshots

_(Add screenshots of your Dashboard, Game Detail Page, and Price Chart here)_
