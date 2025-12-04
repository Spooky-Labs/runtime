# Current Issue: SharedSubscriber Implementation for Pub/Sub Quota

## Problem
Trading agents in GKE were crashing due to Pub/Sub subscription quota exhaustion:
- **Limit**: 10,000 subscriptions per topic/project
- **Root cause**: Each agent created ~70 subscriptions (one per symbol)
- **Result**: 100 agents × 70 symbols = 7,000+ subscriptions, plus orphans from crashed pods

## Solution: SharedSubscriber Pattern
Instead of one subscription per symbol, we now use **one subscription per topic per agent**:
- Old: 70 subscriptions per agent
- New: 2 subscriptions per agent (market-data + crypto-data)

### Architecture
```
Pub/Sub Topic (all symbols) → SharedSubscriber (1 subscription) → Routes to individual feed buffers by symbol
```

### Files Modified
1. **`shared_subscriber.py`** (NEW) - Singleton managing shared subscriptions
2. **`data_feed.py`** - Removed subscription creation, registers with SharedSubscriber
3. **`runner.py`** - Passes agent_id to feeds, cleans up SharedSubscriber on shutdown
4. **`Dockerfile`** - Fixed to handle missing models directory (separate issue)

## Commits Made
1. `e52a3c9` - Add SharedSubscriber to reduce Pub/Sub subscription quota usage
2. `7a60380` - Fix thread safety issue in SharedSubscriber (removed put_notification)
3. `121d604` - Remove all Backtrader attribute access from Pub/Sub callback
4. `355220a` - Fix Dockerfile to handle missing models directory
5. `4ce9410` - Cache buffer references to avoid Backtrader attribute access (THE REAL FIX)
6. `e9b9e9a` - Fix FMEL table name from decisions_v2 to decisions

## Current Status: FIX PUSHED, NEEDS REDEPLOYMENT

### What Was Wrong
Even accessing `feed._data_buffer` from the callback thread goes through Backtrader's
`__getattribute__` machinery, which can trigger internal array access. The solution
is to cache the deque references at registration time (on the main thread).

### Key Technical Detail
The fix in `shared_subscriber.py`:
```python
# At registration time (on main thread):
self.buffers[symbol] = feed._data_buffer  # Cache the deque reference

# In callback (on Pub/Sub thread):
buffer = self.buffers.get(symbol)  # Get cached deque
buffer.append(data)  # Direct deque access, no Backtrader objects touched
```

Reference:
- https://www.backtrader.com/docu/datafeed-develop-general/
- https://community.backtrader.com/topic/543/how-to-create-live-data-feed

## BigQuery Table Name Issue
FMEL analyzer was failing with 404 errors because:
- `runner.py` defaulted FMEL_TABLE to `decisions_v2`
- Only `decisions` table exists in BigQuery `fmel` dataset
- Fixed by changing default to `decisions` (commit `e9b9e9a`)

## Dockerfile Issue (Separate)
Build `fcc5e33f-2445-4a8e-a959-f3acfc8caf2f` failed because:
```
COPY ${MODELS_DIR}* /tmp/detected_models/
COPY failed: no source files were specified
```

Fixed by removing the separate COPY and checking for models in the RUN step instead.
Models are now included via `COPY . .` and checked at `/app/hf_cache/hub`.

## Next Steps
1. Retry the failed agent deployment - it should succeed with the Dockerfile fix
2. Monitor new agents to confirm SharedSubscriber continues working
3. Old agents (using per-symbol subscriptions) will continue working until redeployed
4. Background process `6025da` is cleaning up orphaned `feed-*` subscriptions

## Subscription Count
- Was: 10,131 (over quota)
- Current: ~4,310 (cleanup in progress)
- Target: ~2 per agent (supports 5,000 agents)
