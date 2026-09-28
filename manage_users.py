"""จัดการบัญชีผู้ใช้จากบรรทัดคำสั่ง

    python manage_users.py create --name "ชื่อ" --email a@b.c
    python manage_users.py list
    python manage_users.py adopt-orphans --email a@b.c
    python manage_users.py reset-password --email a@b.c

`adopt-orphans` มีไว้สำหรับ Task ที่สร้างไว้ก่อนจะมีระบบล็อกอิน ซึ่ง uid เป็น NULL
จึงไม่มีใครมองเห็นได้เลยหลังเปิดใช้ระบบยืนยันตัวตน
"""

import argparse
import getpass
import sys

import auth
from db import init_db, session_scope
from models import Task, User

if hasattr(sys.stdout, "reconfigure"):
  sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _ask_password(confirm: bool = True) -> str:
  """รับรหัสผ่านโดยไม่แสดงบนหน้าจอ และไม่รับผ่าน argument

  การส่งรหัสผ่านเป็น argument จะติดอยู่ใน shell history และเห็นได้จาก
  รายการโปรเซส จึงบังคับให้พิมพ์ตอนรันเท่านั้น
  """
  while True:
    password = getpass.getpass("Password (at least 8 characters): ")
    if len(password) < 8:
      print("  Too short, try again")
      continue
    if not confirm:
      return password
    if password != getpass.getpass("Type it again to confirm: "):
      print("  They do not match, try again")
      continue
    return password


def cmd_create(args) -> int:
  try:
    user = auth.create_user(args.name, args.email, _ask_password())
  except ValueError as exc:
    print(f"Error: {exc}")
    return 1
  print(f"User created: {user['name']} <{user['email']}>  uid={user['uid']}")
  return 0


def cmd_list(_args) -> int:
  with session_scope() as session:
    users = session.query(User).order_by(User.created_at).all()
    if not users:
      print("No users yet - create one with: python manage_users.py create ...")
      return 0
    for user in users:
      owned = session.query(Task).filter_by(uid=user.uid).count()
      print(f"{user.email:32s} {user.name:20s} {owned:3d} jobs  uid={user.uid}")

    orphans = session.query(Task).filter(Task.uid.is_(None)).count()
    if orphans:
      print(f"\n{orphans} job(s) have no owner (created before logins existed)")
      print("Hand them to someone with: python manage_users.py adopt-orphans --email ...")
  return 0


def cmd_adopt_orphans(args) -> int:
  with session_scope() as session:
    user = (
        session.query(User)
        .filter_by(email=args.email.strip().lower())
        .one_or_none()
    )
    if user is None:
      print(f"No user with email '{args.email}'")
      return 1

    orphans = session.query(Task).filter(Task.uid.is_(None)).all()
    for task in orphans:
      task.uid = user.uid

  print(f"Moved {len(orphans)} job(s) to {args.email}")
  return 0


def cmd_reset_password(args) -> int:
  with session_scope() as session:
    user = (
        session.query(User)
        .filter_by(email=args.email.strip().lower())
        .one_or_none()
    )
    if user is None:
      print(f"No user with email '{args.email}'")
      return 1
    user.passwd = auth.hash_password(_ask_password())

  print(f"Password changed for {args.email}")
  return 0


def main() -> int:
  parser = argparse.ArgumentParser(description="Manage user accounts")
  sub = parser.add_subparsers(dest="command", required=True)

  create = sub.add_parser("create", help="create a new user")
  create.add_argument("--name", required=True)
  create.add_argument("--email", required=True)
  create.set_defaults(func=cmd_create)

  listing = sub.add_parser("list", help="list the users")
  listing.set_defaults(func=cmd_list)

  adopt = sub.add_parser(
      "adopt-orphans", help="hand ownerless jobs to one user"
  )
  adopt.add_argument("--email", required=True)
  adopt.set_defaults(func=cmd_adopt_orphans)

  reset = sub.add_parser("reset-password", help="set a new password")
  reset.add_argument("--email", required=True)
  reset.set_defaults(func=cmd_reset_password)

  args = parser.parse_args()
  init_db()
  return args.func(args)


if __name__ == "__main__":
  raise SystemExit(main())
