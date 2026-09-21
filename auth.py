"""การยืนยันตัวตนผู้ใช้

ตาราง User ในเอกสารระบุคอลัมน์ `passwd` เป็น string เราคงชื่อคอลัมน์ไว้ตามสเปก
แต่เก็บเฉพาะค่า hash เท่านั้น ไม่เก็บรหัสผ่านดิบ

วิธี hash: sha256 -> base64 -> bcrypt
bcrypt อ่านรหัสผ่านได้สูงสุด 72 ไบต์ และตัดส่วนที่เกินทิ้งเงียบ ๆ ภาษาไทยหนึ่ง
ตัวอักษรกินถึง 3 ไบต์ใน UTF-8 รหัสผ่านไทย 24 ตัวก็ชนเพดานแล้ว จึงย่อด้วย sha256
ก่อนเสมอ (แปลงเป็น base64 ด้วยเพื่อไม่ให้มีไบต์ 0 ซึ่ง bcrypt ใช้เป็นจุดตัดสตริง)
"""

import base64
import hashlib
import os
import secrets

import bcrypt
from fastapi import HTTPException, Request, status

from db import session_scope
from models import User

SESSION_KEY = "uid"


def secret_key() -> str:
  """คีย์สำหรับเซ็น session cookie

  ถ้าไม่ได้ตั้ง SOLAR_SECRET_KEY จะสุ่มใหม่ทุกครั้งที่สตาร์ต ซึ่งแปลว่า
  ผู้ใช้จะหลุด session เมื่อรีสตาร์ตเซิร์ฟเวอร์ — ยอมรับได้ตอนพัฒนา
  แต่บนเซิร์ฟเวอร์จริงต้องตั้งค่านี้ ไม่งั้นทุกคนจะถูก logout ทุกครั้งที่ deploy
  """
  configured = os.environ.get("SOLAR_SECRET_KEY")
  if configured:
    return configured
  print(
      "[auth] ไม่ได้ตั้ง SOLAR_SECRET_KEY — สุ่มคีย์ชั่วคราว"
      " session จะหลุดเมื่อรีสตาร์ต"
  )
  return secrets.token_urlsafe(32)


def _prepare(raw: str) -> bytes:
  return base64.b64encode(hashlib.sha256(raw.encode("utf-8")).digest())


def hash_password(raw: str) -> str:
  return bcrypt.hashpw(_prepare(raw), bcrypt.gensalt()).decode("ascii")


def verify_password(raw: str, hashed: str) -> bool:
  if not hashed:
    return False
  try:
    return bcrypt.checkpw(_prepare(raw), hashed.encode("ascii"))
  except (ValueError, TypeError):
    return False  # ค่าใน DB ไม่ใช่ hash ที่ bcrypt อ่านได้


# -------------------------------------------------------------
# session
# -------------------------------------------------------------
def login_session(request: Request, uid: str) -> None:
  request.session[SESSION_KEY] = uid


def logout_session(request: Request) -> None:
  request.session.clear()


def _unauthorized() -> HTTPException:
  return HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="กรุณาเข้าสู่ระบบก่อน",
  )


def current_user(request: Request) -> dict:
  """FastAPI dependency — คืนข้อมูลผู้ใช้ที่ล็อกอินอยู่ หรือโยน 401"""
  uid = request.session.get(SESSION_KEY)
  if not uid:
    raise _unauthorized()

  with session_scope() as session:
    user = session.get(User, uid)
    if user is None:
      # ผู้ใช้ถูกลบไปแล้วแต่ cookie ยังอยู่
      request.session.clear()
      raise _unauthorized()
    return {"uid": user.uid, "name": user.name, "email": user.email}


# -------------------------------------------------------------
# การสร้าง / ตรวจรหัสผ่านผู้ใช้
# -------------------------------------------------------------
def create_user(name: str, email: str, password: str) -> dict:
  """สร้างผู้ใช้ใหม่ คืนข้อมูลที่ปลอดภัยต่อการส่งออก (ไม่มี passwd)"""
  email = email.strip().lower()
  with session_scope() as session:
    exists = session.query(User).filter_by(email=email).one_or_none()
    if exists is not None:
      raise ValueError(f"อีเมล '{email}' ถูกใช้ไปแล้ว")

    user = User(name=name.strip(), email=email, passwd=hash_password(password))
    session.add(user)
    session.flush()
    return {"uid": user.uid, "name": user.name, "email": user.email}


def authenticate(email: str, password: str) -> dict | None:
  """ตรวจอีเมล + รหัสผ่าน คืน None เมื่อไม่ผ่าน (ไม่บอกว่าผิดตรงไหน)"""
  with session_scope() as session:
    user = (
        session.query(User)
        .filter_by(email=email.strip().lower())
        .one_or_none()
    )
    if user is None:
      # ยังเรียก verify_password กับค่าเปล่าเพื่อให้เวลาที่ใช้ใกล้เคียงกัน
      # ไม่ให้เดาได้จากความเร็วว่าอีเมลนี้มีอยู่จริงหรือไม่
      verify_password(password, hash_password("dummy"))
      return None
    if not verify_password(password, user.passwd):
      return None
    return {"uid": user.uid, "name": user.name, "email": user.email}
