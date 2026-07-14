variable "profile" {
  type    = string
  default = "stigen-io-tasks/sandbox/AdministratorAccess"
}

variable "region_east" {
  type    = string
  default = "us-east-2"
}

variable "region_west" {
  type    = string
  default = "us-west-2"
}

# Ubuntu 24.04 (noble) arm64, looked up 2026-06; override if stale.
variable "ami_east" {
  type    = string
  default = "ami-0be851ac12cc5900e" # us-east-2
}

variable "ami_west" {
  type    = string
  default = "ami-09e85653191bf5ffe" # us-west-2
}

variable "instance_type" {
  type    = string
  default = "c7g.large" # Graviton/arm64; bump to c7g.xlarge if the interpreter needs headroom
}

variable "east_store_count" {
  type    = number
  default = 3
}

variable "west_store_count" {
  type    = number
  default = 2
}

# CIDR allowed to reach SSH (22) and the apiserver (6443). MUST be set to your /32
# for anything beyond a momentary test: `-var allowed_cidr=$(curl -s https://checkip.amazonaws.com)/32`.
variable "allowed_cidr" {
  type    = string
  default = "0.0.0.0/0"
}

# Account id is auto-derived; bucket name must match build-and-stage.sh.
variable "artifact_bucket_region" {
  type    = string
  default = "us-east-2"
}

variable "tags" {
  type = map(string)
  default = {
    project = "cws-apiserver-smoke"
  }
}
