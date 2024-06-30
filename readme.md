API data caching service

To get started, send the JSON config `POST /update` to distribute the received data across tables
example:
```json
{
"users": ["user_id", "username"],
"places": ["place_id", "", "address", "latitude", "longitude"]
}
```
*The first fields (user_id, place_id) will be the key to the table connotation