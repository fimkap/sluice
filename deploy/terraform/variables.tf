variable "region" {
  description = "AWS region. il-central-1 (Tel Aviv) works if the instance types below are offered there."
  type        = string
  default     = "eu-central-1"
}

variable "name" {
  description = "Prefix for resource names."
  type        = string
  default     = "sluice"
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "broker_count" {
  description = "Number of brokers, one per availability zone (the region needs at least this many AZs)."
  type        = number
  default     = 3
}

variable "broker_instance_type" {
  description = "Graviton instance for brokers; the image is built for linux/arm64."
  type        = string
  default     = "m7g.large"
}

variable "bench_instance_type" {
  description = "Instance for running sluicectl and sluice-bench inside the VPC."
  type        = string
  default     = "c7g.xlarge"
}

variable "data_volume_gb" {
  type    = number
  default = 100
}

variable "data_volume_iops" {
  description = "gp3 IOPS (3000 is the free baseline)."
  type        = number
  default     = 3000
}

variable "data_volume_throughput" {
  description = "gp3 throughput in MiB/s (125 is the free baseline)."
  type        = number
  default     = 125
}

variable "image_tag" {
  description = "Tag of the sluice image in the ECR repository this stack creates."
  type        = string
  default     = "latest"
}

variable "fsync" {
  description = "Broker fsync policy: none, interval, always or group."
  type        = string
  default     = "group"
  validation {
    condition     = contains(["none", "interval", "always", "group"], var.fsync)
    error_message = "fsync must be none, interval, always or group."
  }
}

variable "commit_rule" {
  description = "isr (Kafka) or quorum (Raft majority)."
  type        = string
  default     = "isr"
  validation {
    condition     = contains(["isr", "quorum"], var.commit_rule)
    error_message = "commit_rule must be isr or quorum."
  }
}

variable "min_insync" {
  type    = number
  default = 2
}

variable "log_retention_days" {
  type    = number
  default = 14
}
