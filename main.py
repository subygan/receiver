from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
import json
from datetime import datetime
import os
import fcntl
import time
from fastapi.concurrency import asynccontextmanager
import asyncio
from contextlib import contextmanager

app = FastAPI(title="Thread-safe JSON to JSONL Server")

# Configuration
JSONL_FILE = "data.jsonl"
MAX_RETRIES = 3
RETRY_DELAY = 0.1  # seconds


class JSONData(BaseModel):
    """Pydantic model that accepts any JSON data"""
    data: Dict[str, Any]


@contextmanager
def file_lock():
    """
    Context manager for file locking using fcntl.
    Ensures thread-safe file operations.
    """
    lock_file = f"{JSONL_FILE}.lock"
    lock_fd = open(lock_file, 'w+')

    try:
        # Acquire lock
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        yield
    finally:
        # Release lock
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()
        # Clean up lock file
        try:
            os.remove(lock_file)
        except OSError:
            pass


async def append_to_jsonl(data: Dict[str, Any]) -> None:
    """
    Appends JSON data to a JSONL file with timestamp in a thread-safe manner

    Args:
        data (Dict[str, Any]): The JSON data to append
    """
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "data": data
    }

    for attempt in range(MAX_RETRIES):
        try:
            # Run the file operations in a thread pool to avoid blocking
            def write_with_lock():
                with file_lock():
                    # Create file if it doesn't exist
                    if not os.path.exists(JSONL_FILE):
                        with open(JSONL_FILE, 'w') as f:
                            pass

                    # Append the data
                    with open(JSONL_FILE, 'a') as f:
                        f.write(json.dumps(entry) + '\n')
                        # Ensure the data is written to disk
                        f.flush()
                        os.fsync(f.fileno())

            # Execute the file operations in a thread pool
            await asyncio.to_thread(write_with_lock)
            return

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to write to file after {MAX_RETRIES} attempts: {str(e)}"
                )
            await asyncio.sleep(RETRY_DELAY)


@app.post("/append/")
async def append_json(json_data: JSONData):
    """
    Endpoint to receive JSON data and append it to JSONL file

    Returns:
        dict: Success message with timestamp
    """
    try:
        await append_to_jsonl(json_data.data)
        return {
            "status": "success",
            "message": "Data appended successfully",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health/")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)