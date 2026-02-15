export const config = {
  runtime: "edge",
};

export default async function handler(req) {
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  const body = await req.json();
  const { prompt } = body;

  const response = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: "gpt-3.5-turbo",
      stream: true,
      messages: [
        {
          role: "system",
          content: "You are a senior Java developer."
        },
        {
          role: "user",
          content: `
Generate a complete Java class named DataProcessor.
Minimum 120 lines.
Minimum 2500 characters.
Include:
- File reading
- Validation methods
- Error handling
- Multiple helper methods
- Logging
- Comments

${prompt}
`
        }
      ],
      max_tokens: 1800,
      temperature: 0.6
    }),
  });

  const readableStream = new ReadableStream({
    async start(controller) {
      const reader = response.body.getReader();
      const decoder = new TextDecoder();

      // Immediate first chunk (super fast)
      controller.enqueue(
        new TextEncoder().encode(
          `data: {"choices":[{"delta":{"content":"Generating Java code...\\n"}}]}\n\n`
        )
      );

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        controller.enqueue(value);
      }

      controller.enqueue(new TextEncoder().encode("data: [DONE]\n\n"));
      controller.close();
    },
  });

  return new Response(readableStream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
    },
  });
}