"""Public crest URLs. The frontend only ever sees these, never provider URLs.

Images live in S3 at crests/<kind>/<logo_file_id> (written by
etl.src.update_logos). CREST_BASE_URL points at the bucket or a CDN in front
of it; changing it moves every crest without touching the payload shape.
"""
import os

KINDS = ("countries", "leagues", "teams")


def base_url() -> str:
    explicit = os.getenv("CREST_BASE_URL")
    if explicit:
        return explicit.rstrip("/")
    bucket = os.getenv("ASSETS_BUCKET", "ballistics-assets")
    region = os.getenv("AWS_DEFAULT_REGION", "ca-central-1")
    return f"https://{bucket}.s3.{region}.amazonaws.com"


def crest_url(kind: str, logo_file_id) -> str | None:
    """URL for one crest, or None when there is none (frontend shows initials)."""
    if kind not in KINDS:
        raise ValueError(f"unknown crest kind {kind!r}")
    if not logo_file_id:
        return None
    return f"{base_url()}/crests/{kind}/{logo_file_id}"
