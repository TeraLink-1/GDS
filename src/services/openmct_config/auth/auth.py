import time
import secrets
import httpx
import pyotp
import bcrypt
from pathlib import Path

HERE = Path(__file__).parent

from fastapi import FastAPI, Request, Response, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.exception_handlers import http_exception_handler
from starlette.exceptions import HTTPException as StarletteHTTPException

app = FastAPI()

@app.exception_handler(StarletteHTTPException)
async def redirect_401(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 401:
        return RedirectResponse("/login", status_code=302)
    return await http_exception_handler(request, exc)

OPENMCT_ORIGIN = "http://localhost:3000"
SESSION_TTL = 3600

# --- Hardcoded test user ---
# Generate a real bcrypt hash so the login flow is exercised properly.
# Change these before real use.
TEST_TOTP_SECRET = "UGTIZLNBCRG6VZSLGELXZXHZ7JBUVZJI"  # fixed for local testing
TEST_USERS = {
    "admin": {
        "password_hash": bcrypt.hashpw(b"password123", bcrypt.gensalt()),
        "totp_secret": TEST_TOTP_SECRET,
    }
}

@app.on_event("startup")
async def print_totp_uri():
    uri = pyotp.TOTP(TEST_TOTP_SECRET).provisioning_uri("admin", issuer_name="TERALINK-1")
    print("\n=== TEST MODE ===")
    print(f"Username : admin")
    print(f"Password : password123")
    print(f"TOTP URI : {uri}")
    print("Scan the URI with Google Authenticator / Authy, or use:")
    print(f"  python3 -c \"import pyotp; print(pyotp.TOTP('{TEST_TOTP_SECRET}').now())\"")
    print("=================\n")

sessions: dict[str, dict] = {}


def get_user(username: str):
    return TEST_USERS.get(username)


def require_session(request: Request):
    sid = request.cookies.get("session_id")
    session = sessions.get(sid) if sid else None
    if not session or session["expires"] < time.time():
        if sid and sid in sessions:
            del sessions[sid]
        raise HTTPException(status_code=401)
    return session


@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return (HERE / "login.html").read_text()


@app.post("/login")
async def login(
    username: str = Form(...),
    password: str = Form(...),
    totp_code: str = Form(...),
):
    user = get_user(username)
    if not user or not bcrypt.checkpw(password.encode(), user["password_hash"]):
        return HTMLResponse("<p>Invalid credentials.</p><a href='/login'>Back</a>", status_code=401)

    if not pyotp.TOTP(user["totp_secret"]).verify(totp_code, valid_window=1):
        return HTMLResponse("<p>Invalid 2FA code.</p><a href='/login'>Back</a>", status_code=401)

    sid = secrets.token_urlsafe(32)
    sessions[sid] = {"user": username, "expires": time.time() + SESSION_TTL}

    resp = RedirectResponse("/", status_code=302)
    resp.set_cookie("session_id", sid, httponly=True, samesite="strict", max_age=SESSION_TTL)
    return resp


@app.post("/logout")
async def logout(request: Request):
    sid = request.cookies.get("session_id")
    if sid and sid in sessions:
        del sessions[sid]
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("session_id")
    return resp


SKIP_HEADERS = {"host", "content-length", "transfer-encoding"}

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def proxy(request: Request, path: str, session=Depends(require_session)):
    url = f"{OPENMCT_ORIGIN}/{path}"
    if request.url.query:
        url += f"?{request.url.query}"

    headers = {k: v for k, v in request.headers.items() if k.lower() not in SKIP_HEADERS}

    async with httpx.AsyncClient() as client:
        upstream = await client.request(
            method=request.method,
            url=url,
            headers=headers,
            content=await request.body(),
        )

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers={k: v for k, v in upstream.headers.items() if k.lower() not in {"transfer-encoding"}},
    )