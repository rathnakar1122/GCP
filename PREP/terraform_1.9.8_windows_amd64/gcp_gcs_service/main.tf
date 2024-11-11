provider "google" {
  credentials = file("<PATH_TO_YOUR_SERVICE_ACCOUNT_JSON>")
  project     = "<YOUR_PROJECT_ID>"
  region      = "<YOUR_REGION>"
}

resource "google_storage_bucket" "my_bucket" {
  name     = "<YOUR_BUCKET_NAME>"  # Must be globally unique
  location = "US"                   # Choose your desired location

  versioning {
    enabled = true  # Enable versioning if desired
  }

  lifecycle {
    prevent_destroy = true  # Prevent accidental deletion
  }

  labels = {
    environment = "dev"
    owner       = "user"
  }
}

