output "ecr_repository_url" {
  description = "Push the arm64 image here (see deploy/terraform/README.md)."
  value       = aws_ecr_repository.sluice.repository_url
}

output "bootstrap" {
  description = "Broker addresses for clients inside the VPC."
  value       = join(",", [for ip in local.broker_ips : "${ip}:9092"])
}

output "broker_instance_ids" {
  value = aws_instance.broker[*].id
}

output "bench_instance_id" {
  value = aws_instance.bench.id
}

output "connect_bench" {
  description = "Open a shell on the bench host (needs the Session Manager plugin)."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.bench.id}"
}

output "logs" {
  value = "aws logs tail ${aws_cloudwatch_log_group.brokers.name} --region ${var.region} --follow"
}
