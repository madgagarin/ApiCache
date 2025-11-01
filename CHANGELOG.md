# Changelog

## [1.0.0] - 2025-11-01

This is the first stable release after a comprehensive modernization of the project.

### Added

- **Automatic Cache Refresh (TTL):** Implemented a Time-To-Live mechanism. The cache now automatically refreshes in the background if the data is stale. The TTL is configurable via the `CACHE_TTL_SECONDS` environment variable.
- **Automated Tests:** Created a full test suite using `pytest` to verify the core API logic, including filtering, searching, and updates, as well as the security module.

### Changed

- **Dependencies Updated:** All project dependencies (`aiohttp`, `orjson`, `aiosqlite`) have been updated to the latest stable versions for improved performance and security.
- **Improved Documentation:** The `readme.md` file has been completely rewritten and translated into English, providing detailed instructions for setup, configuration, and API usage.
- **Code Comments:** Added comprehensive English comments and docstrings throughout the codebase to improve readability and maintainability.
- **Project Configuration:** An improved `.gitignore` file has been added to exclude the database and other generated files from the repository.

### Fixed

- **SQL Injection Vulnerability:** Added a new `security.py` module to sanitize user-provided table and column names. This closes a critical vulnerability that could have allowed arbitrary SQL execution.
