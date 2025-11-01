# API Caching Service

This service acts as a caching proxy for an external API. It fetches data from a specified source, processes it according to a user-provided schema, and stores it in a local SQLite database. Subsequent requests serve data from the cache, reducing the load on the source service and speeding up responses.

A key feature of this service is its ability to not just cache data but also enrich it with advanced filtering and full-text search capabilities.

## Core Features

- **Flexible Data Schema:** Define the structure of your tables and columns via a simple JSON configuration.
- **Automatic Cache Updates:** A built-in Time-To-Live (TTL) mechanism automatically refreshes the cache when data becomes stale.
- **Advanced Filtering and Search:** The service supports filtering by any field and provides full-text search across all data.
- **Easy Deployment:** The application is containerized using Docker for a quick and straightforward setup.

## Setup and Launch

### System Requirements

- Docker

### Running with Docker

1.  **Build the Docker image:**
    ```bash
    docker build -t api-cache-service .
    ```

2.  **Run the container:**
    ```bash
    docker run -p 8080:8080 \
      -e SOURCE_URL="your-source-api.com" \
      -e SOURCE_PATH="/data" \
      -e CACHE_TTL_SECONDS=3600 \
      api-cache-service
    ```

### Environment Variables

- `SOURCE_URL`: The URL of the external API source (without `http://`).
- `SOURCE_PATH`: The path to the data endpoint on the external API.
- `CACHE_TTL_SECONDS` (optional): The cache's time-to-live in seconds. Defaults to `3600` (1 hour).

## API Reference

### `POST /update`

**Purpose:** Updates the database schema and immediately triggers a cache refresh.

**Request Body:**
```json
{
  "users": ["user_id", "username", "email"],
  "posts": ["post_id", "title", "body", "user_id"]
}
```
- The JSON keys (`users`, `posts`) become the table names.
- The first element in the value array (`user_id`, `post_id`) is treated as the primary key.

**Response:**
```
OK

DB configuration:
users: ['user_id', 'username', 'email']
posts: ['post_id', 'title', 'body', 'user_id']

Recorded:
users: 100
posts: 500
```

### `GET /`

**Purpose:** Returns all cached data.

**Example Response:**
```json
[
  {
    "user_id": "1",
    "username": "testuser",
    "email": "test@example.com",
    "post_id": "101",
    "title": "Test Post",
    "body": "This is a test post.",
    "posts_user_id": "1"
  },
  ...
]
```

### `GET /{search_text}`

**Purpose:** Performs a full-text search across all fields in all tables.

**Example Request:**
`GET /Test Post`

### `POST /`

**Purpose:** Allows filtering data by specific fields and performing a search.

**Request Body (form-data):**
- `username`: `testuser`
- `email`: `test@example.com`
- `searchstring`: `test`

## Testing

To run the automated tests, execute the following commands:
```bash
pip install -r requirements-dev.txt
pytest
```
