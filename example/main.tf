provider "postgresql" {
  host     = "127.0.0.1"
  port     = 5432
  database = "db"
  username = "user"
  password = "password"
  sslmode  = "disable"

  # version  = "~> 0.2"
}

resource "postgresql_table" "items" {
  name = "items"
}
