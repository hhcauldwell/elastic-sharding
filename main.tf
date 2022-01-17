resource "google_cloudbuild_trigger" "test" {
  name     = "elastic-sharding-test"
  project  = "kv-cloud-run-3"
  disabled = false
  filename = "cloudbuild.yaml"
  github {
    owner = "hhcauldwell"
    name  = "elastic-sharding"
    push {
      branch = "^test$"
    }
  }
  substitutions = {
    _SERVICE       = "elastic-sharding-test"
    _CPUS          = 1
    _MEMORY        = "1G"
    _MIN_INSTANCES = 10
    _MAX_INSTANCES = 10
  }
}
