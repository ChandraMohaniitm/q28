import os
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

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

app = FastAPI()

class PromptRequest(BaseModel):
    prompt: str
    stream: bool = True

# Create client ONCE (important for speed)
client = httpx.AsyncClient(timeout=25.0)

async def stream_llm(prompt: str):

    # IMMEDIATE FIRST CHUNK (no sleep)
    yield 'data: {"choices":[{"delta":{"content":"Generating Java code...\\n"}}]}\n\n'

    payload = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {
                "role": "system",
                "content": "You are a senior Java developer."
            },
            {
                "role": "user",
                "content": f"""
Generate a complete Java class named DataProcessor.

Requirements:
- At least 100 lines
- At least 2500 characters
- Include:
  - BufferedReader file reading
  - Validation methods
  - Multiple helper methods
  - Try-catch error handling
  - Logging statements
  - Comments
  - Clean formatting

{prompt}
"""
            }
        ],
        "stream": True,
        "max_tokens": 1800,
        "temperature": 0.6
    }

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        async with client.stream(
            "POST",
            f"{API_BASE}/chat/completions",
            headers=headers,
            json=payload,
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

    except Exception:
        yield 'data: {"error":"Streaming failed"}\n\n'
        yield "data: [DONE]\n\n"

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
        },
    )

@app.get("/health")
async def health():
    return {"status": "ok"}