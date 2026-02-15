"""
Streaming LLM API (SSE) – Compatible with AIPIPE or OpenAI
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

# ------------------------
# Load Environment
# ------------------------

AIPIPE_TOKEN = os.getenv("AIPIPE_TOKEN")
AIPIPE_BASE_URL = os.getenv("AIPIPE_BASE_URL")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Decide which provider to use
if AIPIPE_TOKEN:
    API_KEY = AIPIPE_TOKEN
    API_BASE = AIPIPE_BASE_URL or "https://aipipe.org/openai/v1"
elif OPENAI_API_KEY:
    API_KEY = OPENAI_API_KEY
    API_BASE = "https://api.openai.com/v1"
else:
    raise ValueError("No API key configured (AIPIPE_TOKEN or OPENAI_API_KEY required)")

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


# ------------------------
# Streaming Generator
# ------------------------

async def stream_llm(prompt: str):

    # Immediate flush
    yield 'data: {"choices":[{"delta":{"content":""}}]}\n\n'
    await asyncio.sleep(0)

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }

    enhanced_prompt = f"""
Generate Java code for a data processor class.

Requirements:
- At least 67 lines
- At least 1700 characters
- Include file reading, validation, error handling, logging
- Production-style formatting

User request:
{prompt}
"""

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "You are an expert Java developer."},
            {"role": "user", "content": enhanced_prompt}
        ],
        "stream": True,
        "max_tokens": 2000,
        "temperature": 0.7
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
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


# ------------------------
# Single Streaming Endpoint
# ------------------------

@app.post("/stream")
async def stream_endpoint(request: PromptRequest):

    if not request.stream:
        raise HTTPException(status_code=400, detail="Streaming must be enabled")

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