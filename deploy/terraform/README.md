# Deploying sluice on AWS

This stack runs three brokers on Graviton EC2 instances, one per availability zone, each with its own
encrypted gp3 data volume, plus a bench host in the same VPC. Images come from an ECR repository the stack
creates; logs go to CloudWatch; you reach the hosts with SSM Session Manager (no SSH keys, no port 22).

Brokers are stateful: each needs its own disk and a stable address, since peers are configured by IP. That
is why they run on EC2 rather than Fargate. The stack has been checked with `terraform validate`
(`make tf-validate` runs it in Docker); it has not been applied as part of this repository, so expect to
adjust instance types to what your region offers.

## Cost

At on-demand prices in eu-central-1, three `m7g.large` brokers, one `c7g.xlarge` bench host and 3 × 100 GB
of gp3 cost roughly $10 a day. Destroy the stack when you are done.

## Steps

You need Terraform 1.6+, the AWS CLI with credentials, Docker with buildx, and the Session Manager plugin.

**1. Create the stack** (the brokers start pulling an image that doesn't exist yet; they retry on boot):

```
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # adjust the region and settings
terraform init
terraform apply
```

**2. Build the arm64 image and push it** to the repository from the output:

```
REPO=$(terraform output -raw ecr_repository_url)
aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin ${REPO%%/*}
docker buildx build --platform linux/arm64 -t $REPO:latest --push ../..
```

Then restart the brokers so they pull it (or `terraform apply -replace=aws_instance.broker[0]` and so on):

```
for id in $(terraform output -json broker_instance_ids | tr -d '[]"' | tr , ' '); do
  aws ssm send-command --instance-ids $id --document-name AWS-RunShellScript \
    --parameters 'commands=["cloud-init clean --reboot"]'
done
```

**3. Use the cluster from the bench host:**

```
$(terraform output -raw connect_bench)
# on the bench host:
REPO=...; BOOT=...   # from terraform output ecr_repository_url and bootstrap
aws ecr get-login-password | sudo docker login --username AWS --password-stdin ${REPO%%/*}
sudo docker run --rm --network host --entrypoint /sluicectl $REPO -bootstrap $BOOT create-topic -topic orders -partitions 6 -rf 3
sudo docker run --rm --network host --entrypoint /sluicectl $REPO -bootstrap $BOOT topics
```

**4. Measure fsync on EBS.** The experiments start their own brokers, so run them on the bench host to see
what group commit buys on gp3 compared to the numbers in the main README:

```
sudo docker run --rm -v /var/tmp:/data --entrypoint /sluice-bench $REPO experiment durability -dir /data
```

**5. Watch it:** `$(terraform output -raw logs)` tails the broker logs; each broker serves Prometheus
metrics on port 9100 inside the VPC.

**6. Destroy it:** `terraform destroy`.

## What production would add

Private subnets with VPC endpoints (or a NAT gateway), an Auto Scaling group per broker with the volume
reattached on replacement, a Prometheus scraper, and TLS between clients and brokers. Leader failover is not
implemented in sluice itself (see `docs/design.md`).
