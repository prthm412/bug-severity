from fastapi import FastAPI, Request

app = FastAPI(title="Bug Severity Service")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/webhook/github")
async def webhook(request: Request):
    payload = await request.json()
    print("=== GOT EVENT ===")
    print(payload)
    return {"ok": True}