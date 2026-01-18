FROM dunglas/frankenphp:php8.4-bookworm

# Allow Composer to run as root
ENV COMPOSER_ALLOW_SUPERUSER=1

# Use production PHP settings
RUN mv "$PHP_INI_DIR/php.ini-production" "$PHP_INI_DIR/php.ini"

# Install PHP extensions using FrankenPHP's built-in installer
# This is the CORRECT method for dunglas/frankenphp images
RUN install-php-extensions \
    pdo_pgsql \
    zip \
    pcntl \
    exif \
    gd \
    intl \
    bcmath \
    opcache

# Install Node.js 20 from NodeSource (Debian's default is too old)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Composer from official image (FrankenPHP doesn't include it)
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

# Copy composer files first for better layer caching
COPY composer.json composer.lock ./

# Install PHP dependencies (production mode)
# Skip post-install scripts - artisan doesn't exist yet
RUN composer install \
    --no-dev \
    --optimize-autoloader \
    --no-interaction \
    --no-progress \
    --prefer-dist \
    --no-scripts

# Copy package files
COPY package.json package-lock.json ./

# Install Node dependencies
RUN npm ci --omit=dev

# Copy application code
COPY . .

# NOW run Composer post-install scripts (artisan exists now)
RUN composer run-script post-autoload-dump

# Run additional autoload optimization
RUN composer dump-autoload --optimize

# Build frontend assets for production
RUN npm run build

# Create required Laravel directories
RUN mkdir -p storage/framework/{sessions,views,cache} \
    && mkdir -p storage/logs \
    && mkdir -p bootstrap/cache

# Laravel production optimizations
RUN php artisan config:clear \
    && php artisan route:clear \
    && php artisan view:clear

# Set proper permissions
RUN chmod -R 775 storage bootstrap/cache \
    && chown -R www-data:www-data storage bootstrap/cache

# Configure FrankenPHP to serve Laravel
# The default Caddyfile in FrankenPHP expects app in /app with public in /app/public
ENV SERVER_NAME=:8080

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/up || exit 1

# Start FrankenPHP (not artisan serve - FrankenPHP handles this)
CMD ["frankenphp", "run", "--config", "/etc/caddy/Caddyfile"]
