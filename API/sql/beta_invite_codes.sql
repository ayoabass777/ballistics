-- Private beta admission codes. Store only keyed hashes, never plaintext codes.
CREATE TABLE IF NOT EXISTS app.beta_invite_codes (
    code_hash TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    claimed_at TIMESTAMPTZ,
    clerk_invitation_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT beta_invite_claim_consistent CHECK ((claimed_at IS NULL) = (clerk_invitation_id IS NULL))
);
CREATE INDEX IF NOT EXISTS beta_invite_codes_email_idx ON app.beta_invite_codes (lower(email));
