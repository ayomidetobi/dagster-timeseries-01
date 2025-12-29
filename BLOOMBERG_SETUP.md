# Bloomberg Connection Setup

## Overview

The Bloomberg connection is established automatically by the `xbbg` library when you make your first API call. The connection happens in the `_fetch_single_ticker()` method in `dagster_clickhouse/bloomberg.py` when `blp.bdh()` is called.

## Connection Modes

### 1. Desktop API (Default - Bloomberg Terminal)

**Requirements:**
- Bloomberg Terminal must be installed and running
- Bloomberg Terminal must be logged in
- No additional configuration needed

**How it works:**
- `xbbg` automatically detects the local Bloomberg Terminal
- Connection is established on first API call
- Uses the Bloomberg Open API (BLPAPI) that comes with Bloomberg Terminal

**Setup:**
1. Install Bloomberg Terminal
2. Log in to Bloomberg Terminal
3. Keep Terminal running while using the ingestion asset
4. No code changes needed - it works automatically

### 2. Server API

**Requirements:**
- Bloomberg Server API access
- Server host and port information

**Configuration:**

Option 1: Environment Variables
```bash
export BLOOMBERG_HOST=your-bloomberg-server.com
export BLOOMBERG_PORT=8194
```

Option 2: Resource Configuration
```python
# In definitions.py or resource config
resources = {
    "bloomberg": BloombergResource(
        host="your-bloomberg-server.com",
        port=8194
    ),
}
```

### 3. Cloud API

**Requirements:**
- Bloomberg API credentials
- API key and authentication setup

**Configuration:**
- Follow Bloomberg API documentation for cloud authentication
- Configure credentials via environment variables or Bloomberg API settings

## Testing the Connection

The ingestion asset automatically tests the connection before starting:

```python
# This happens automatically in ingest_bloomberg_data_async
bloomberg.test_connection()
```

You can also test manually:
```python
from dagster_clickhouse.bloomberg import BloombergResource

bloomberg = BloombergResource()
if bloomberg.test_connection():
    print("Connected successfully!")
else:
    print("Connection failed - check Bloomberg Terminal or Server API")
```

## Where Connection Happens in Code

1. **Connection Test**: `bloomberg.test_connection()` in `bloomberg.py` line 50
2. **Actual Connection**: `blp.bdh()` call in `_fetch_single_ticker()` method, line ~102 in `bloomberg.py`
3. **First Call**: When `ingest_bloomberg_data_async` asset runs and calls `bloomberg.fetch_multiple_series()`

## Troubleshooting

### Connection Failed Errors

1. **Desktop API Issues:**
   - Ensure Bloomberg Terminal is running
   - Check that you're logged in to Bloomberg Terminal
   - Verify BLPAPI is installed (comes with Terminal)

2. **Server API Issues:**
   - Verify host and port are correct
   - Check network connectivity to Bloomberg server
   - Ensure you have Bloomberg Server API access

3. **Common Errors:**
   - `Connection refused`: Bloomberg Terminal not running or wrong host/port
   - `Authentication failed`: Not logged in or invalid credentials
   - `Timeout`: Network issues or Bloomberg server unavailable

## Environment Variables

You can configure the connection using these environment variables:

```bash
# For Server API mode
BLOOMBERG_HOST=your-server.com
BLOOMBERG_PORT=8194

# Connection timeout (milliseconds)
BLOOMBERG_TIMEOUT=30000
```

## Example Usage

```python
# The connection is automatic - just use the resource
from dagster_clickhouse.bloomberg import BloombergResource

bloomberg = BloombergResource()

# Connection happens on first API call
data = await bloomberg.get_bloomberg_data_async(
    ticker="AAPL US Equity",
    field="PX_LAST",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31)
)
```

