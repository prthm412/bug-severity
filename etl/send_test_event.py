import requests

payload = {
    "repository": {"full_name": "yourfork/example"},
    "commits": [
        {
            "id": "abc123",
            "timestamp": "2025-01-01T12:00:00Z",
            "message": "fix: sample commit for pipeline",
            "author": {"username": "you"}
        }
    ]
}

r = requests.post("http://localhost:8000/webhook/github",
                  json=payload,
                  headers={"X-Github-Event": "push"})

print(r.status_code, r.text)