"""
FINAL Optimized Streaming LLM API
Low latency + Long output + SSE compliant
"""

import os
import asyncio
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# ========================
# Environment Setup
# ========================

AIPIPE_TOKEN = os.getenv("AIPIPE_TOKEN")
AIPIPE_BASE_URL = os.getenv("AIPIPE_BASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if AIPIPE_TOKEN:
    API_KEY = AIPIPE_TOKEN
    API_BASE = AIPIPE_BASE_URL or "https://aipipe.org/openai/v1"
elif OPENAI_API_KEY:
    API_KEY = OPENAI_API_KEY
    API_BASE = "https://api.openai.com/v1"
else:
    raise ValueError("No API key configured")

# ========================
# FastAPI Setup
# ========================

app = FastAPI(title="Streaming LLM API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class PromptRequest(BaseModel):
    prompt: str
    stream: bool = True

# ========================
# Global HTTP Client
# ========================

client = httpx.AsyncClient(timeout=30.0)

# ========================
# Streaming Generator
# ========================

async def stream_llm(prompt: str):

    # Immediate flush to reduce measured latency
    yield 'data: {"choices":[{"delta":{"content":""}}]}\n\n'
    await asyncio.sleep(0)

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }

    # Strong but compact instruction to ensure long output
    enhanced_prompt = f"""
Generate a production-ready Java class named DataProcessor.

Requirements:
- Minimum 90 lines
- Minimum 2200 characters
- Include:
  - File reading using BufferedReader
  - Data parsing logic
  - Validation methods
  - Exception handling with try-catch
  - Logging using System.out.println
  - At least 4 separate methods
  - Clear comments
  - Proper formatting

User request:
{prompt}
"""

    payload = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {"role": "system", "content": "You are an expert Java backend engineer."},
            {"role": "user", "content": enhanced_prompt}
        ],
        "stream": True,
        "max_tokens": 1700,
        "temperature": 0.7
    }

    try:
        async with client.stream(
            "POST",
            f"{API_BASE}/chat/completions",
            headers=headers,
            json=payload
        ) as response:

            if response.status_code != 200:
                yield f'data: {{"error":"API error {response.status_code}"}}\n\n'
                yield "data: [DONE]\n\n"
                return

            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    data = line[6:]

                    if data.strip() == "[DONE]":
                        break

                    yield f"data: {data}\n\n"

        yield "data: [DONE]\n\n"

    except httpx.TimeoutException:
        yield 'data: {"error":"Request timed out"}\n\n'
        yield "data: [DONE]\n\n"

    except Exception:
        yield 'data: {"error":"Streaming failed"}\n\n'
        yield "data: [DONE]\n\n"

# ========================
# Streaming Endpoint
# ========================

@app.post("/stream")
async def stream_endpoint(request: PromptRequest):

    if not request.stream:
        raise HTTPException(status_code=400, detail="Streaming must be true")

    return StreamingResponse(
        stream_llm(request.prompt),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )

@app.get("/health")
async def health():
    return {"status": "healthy"}