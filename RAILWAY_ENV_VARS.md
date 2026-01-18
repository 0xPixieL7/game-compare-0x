# Railway Environment Variables

Copy these into Railway → Variables tab:

```bash
# App Configuration
APP_NAME=GameCompare
APP_ENV=production
APP_DEBUG=false
APP_URL=https://not-me-making-a-domain.com
APP_KEY=base64:4ekSqVl8M8HrZfi4R8wQBwiCcpZml8zn7oE0rNGyD9Q=

# Database (Supabase PostgreSQL)
DB_CONNECTION=pgsql
DB_HOST=2a05:d016:571:a40d:93e:4d2a:7aab:1321
DB_PORT=5432
DB_DATABASE=postgres
DB_USERNAME=postgres
DB_PASSWORD=Hewhoremains0#
DB_SSLMODE=prefer

# Session & Cache
SESSION_DRIVER=database
SESSION_LIFETIME=120
CACHE_STORE=database
QUEUE_CONNECTION=database

# IGDB/Twitch API
IGDB_CLIENT_ID=xyt5iwkxcdyqriam7esrexyrap0352
IGDB_CLIENT_SECRET=wcj9ikhr0jzjfbjvt1vdk0xjckecbk
IGDB_WEBHOOK_SECRET=wcj9ikhr0jzjfbjvt1vdk0xjckecbkxx

# Vite
VITE_APP_NAME=GameCompare

# Optional: Skip migrations if already run
RAILPACK_SKIP_MIGRATIONS=false
```

## After setting env vars:

1. Railway → Deployments → Deploy latest commit
2. Wait for build to complete
3. Visit https://not-me-making-a-domain.com
