"""Create a single-use beta code: python -m API.issue_beta_invite person@example.com"""
import argparse
from pydantic import TypeAdapter, EmailStr

from API.core.database import SessionLocal
from API.beta_invites import issue_code


def main() -> None:
    parser = argparse.ArgumentParser(description="Issue one email-bound Ballistics beta code")
    parser.add_argument("email")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    if not 1 <= args.days <= 30:
        parser.error("--days must be between 1 and 30")
    email = str(TypeAdapter(EmailStr).validate_python(args.email))
    with SessionLocal() as db:
        code = issue_code(db, email, args.days)
    print(f"Email: {email}\nCode: {code}\nExpires in: {args.days} days")


if __name__ == "__main__":
    main()
