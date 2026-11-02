CREATE TABLE lifecycle_credential_fences (
  credential_key VARCHAR(64) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  actor_member_id BIGINT NOT NULL,
  account_uid VARCHAR(32) NOT NULL,
  revision BIGINT NOT NULL,
  first_request_ref VARCHAR(26) NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_key)
);

CREATE TABLE lifecycle_fence_requests (
  request_ref VARCHAR(26) NOT NULL,
  credential_key VARCHAR(64) NOT NULL,
  identity_json TEXT NOT NULL,
  expected_boot_id VARCHAR(26) NOT NULL,
  armed_boot_id VARCHAR(26) NOT NULL,
  fence_revision BIGINT NOT NULL,
  armed_at_ms BIGINT NOT NULL,
  params_hash VARCHAR(64) NOT NULL,
  PRIMARY KEY (request_ref)
);
