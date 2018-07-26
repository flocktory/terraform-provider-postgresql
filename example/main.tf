provider "postgresql" {
  host     = "127.0.0.1"
  port     = 5432
  database = "db"
  username = "user"
  password = "password"
  sslmode  = "disable"

  # version  = "~> 0.2"
}

resource "postgresql_table" "campaign_sequences" {
  name = "campaign_sequences"

  column {
    name = "id"
    type = "int"

    #default = "nextval('campaign_sequences_id_seq'::regclass)"
  }

  column {
    name = "site_id"
    type = "int"
  }

  column {
    name       = "state"
    type       = "varchar"
    max_length = "255"
  }

  column {
    name       = "campaign_type"
    type       = "varchar"
    max_length = "255"
  }

  column {
    name    = "options"
    type    = "jsonb"
    default = "'{}'::jsonb"
    is_null = true
  }

  column {
    name    = "created_at"
    type    = "timestamp"
    default = "now()"
  }

  column {
    name    = "updated_at"
    type    = "timestamp"
    default = "now()"
  }
}

resource "postgresql_table" "items" {
  name = "items3"

  column {
    name = "id"
    type = "int"
  }

  column {
    name       = "description"
    type       = "varchar"
    max_length = "255"
  }
}
