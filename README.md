# Game Compare 🎮

[![CI](https://github.com/lowkey/game-compare/actions/workflows/ci.yml/badge.svg)](https://github.com/lowkey/game-compare/actions/workflows/ci.yml)

**Game Compare** is a premium game discovery and comparison platform that leverages real-time data to help gamers find the best prices, compare cross-platform availability, and discover new titles through a curated, high-performance interface.

## 🚀 Features

- **Real-Time Price Comparison**: Compare game prices across major retailers (Steam, PlayStation Store, Xbox Store, etc.) to ensure you get the best deal.
- **Dynamic Spotlight**: Discover trending and top-rated games through our algorithmic spotlight feature, powered by real-time metrics.
- **Cross-Platform Availability**: Easily check which platforms a game is available on.
- **Premium UI/UX**: Experience a fluid, responsive, and visually stunning interface built with React, Tailwind CSS, and Framer Motion.
- **Bitcoin Price Tracking**: Compare game value against Bitcoin prices for a unique economic perspective.
- **Automated Transition Effects**: Enjoy seamless navigation with custom transition animations (like the "Pokemon" battle transition).

## 🛠️ Tech Stack

- **Backend**: Laravel 11, PHP 8.3
- **Frontend**: React, Inertia.js, TypeScript
- **Styling**: Tailwind CSS 4.0
- **Database**: PostgreSQL
- **Testing**: Pest (PHP), ESLint, Prettier

## 📦 Installation & Setup

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/lowkey/game-compare.git
    cd game-compare
    ```

2.  **Install dependencies:**

    ```bash
    composer install
    npm install
    ```

3.  **Environment Setup:**

    ```bash
    cp .env.example .env
    php artisan key:generate
    ```

4.  **Database Setup:**
    Configure your database credentials in `.env`, then run:

    ```bash
    php artisan migrate
    ```

5.  **Build Assets:**

    ```bash
    npm run build
    ```

6.  **Run the Application:**
    ```bash
    npm run dev
    php artisan serve
    ```

## ✅ Continuous Integration

This project uses **GitHub Actions** for Continuous Integration to ensure code quality and stability.

- **Tests**: Automated PHPUnit/Pest tests run on every push.
- **Linting**: Code style is enforced using Laravel Pint (PHP) and ESLint/Prettier (JS/TS).
- **Builds**: Frontend assets are compiled to verify build integrity.

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1.  Fork the repository.
2.  Create a new feature branch (`git checkout -b feature/amazing-feature`).
3.  Commit your changes (`git commit -m 'Add some amazing feature'`).
4.  Push to the branch (`git push origin feature/amazing-feature`).
5.  Open a Pull Request.

## 📝 License

This project is open-sourced software licensed under the [MIT license](https://opensource.org/licenses/MIT).
