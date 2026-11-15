# Brokers are stateful: each needs a stable address (peers are configured by IP) and its own disk.
# That is why they run on EC2 with a gp3 volume each, one per AZ, rather than on Fargate.

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

locals {
  broker_ips = [for i in range(var.broker_count) : cidrhost(aws_subnet.public[i].cidr_block, 10)]
  peers      = join(",", [for i, ip in local.broker_ips : "${i + 1}=${ip}:9092"])
  image      = "${aws_ecr_repository.sluice.repository_url}:${var.image_tag}"
}

resource "aws_ebs_volume" "data" {
  count             = var.broker_count
  availability_zone = local.azs[count.index]
  size              = var.data_volume_gb
  type              = "gp3"
  iops              = var.data_volume_iops
  throughput        = var.data_volume_throughput
  encrypted         = true
  tags              = { Name = "${var.name}-data-${count.index + 1}" }
}

resource "aws_instance" "broker" {
  count                  = var.broker_count
  ami                    = data.aws_ssm_parameter.al2023_arm64.value
  instance_type          = var.broker_instance_type
  subnet_id              = aws_subnet.public[count.index].id
  private_ip             = local.broker_ips[count.index]
  vpc_security_group_ids = [aws_security_group.cluster.id]
  iam_instance_profile   = aws_iam_instance_profile.node.name

  metadata_options {
    http_tokens = "required" # IMDSv2 only
  }
  root_block_device {
    volume_type = "gp3"
    volume_size = 16
    encrypted   = true
  }
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    region      = var.region
    registry    = split("/", aws_ecr_repository.sluice.repository_url)[0]
    image       = local.image
    log_group   = aws_cloudwatch_log_group.brokers.name
    node_id     = count.index + 1
    private_ip  = local.broker_ips[count.index]
    peers       = local.peers
    volume_id   = replace(aws_ebs_volume.data[count.index].id, "-", "")
    fsync       = var.fsync
    commit_rule = var.commit_rule
    min_insync  = var.min_insync
  })
  user_data_replace_on_change = true
  tags                        = { Name = "${var.name}-broker-${count.index + 1}" }
}

resource "aws_volume_attachment" "data" {
  count       = var.broker_count
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.data[count.index].id
  instance_id = aws_instance.broker[count.index].id
}

# A host inside the VPC for sluicectl and sluice-bench. It has its own gp3 root volume, so running
# `sluice-bench experiment durability` here measures fsync on EBS.
resource "aws_instance" "bench" {
  ami                    = data.aws_ssm_parameter.al2023_arm64.value
  instance_type          = var.bench_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.cluster.id]
  iam_instance_profile   = aws_iam_instance_profile.node.name
  metadata_options {
    http_tokens = "required"
  }
  root_block_device {
    volume_type = "gp3"
    volume_size = 50
    encrypted   = true
  }
  user_data = <<-EOT
    #!/bin/bash
    dnf install -y docker
    systemctl enable --now docker
  EOT
  tags      = { Name = "${var.name}-bench" }
}
