terraform {
  required_version = ">= 1.5"
  required_providers {
    aws   = { source = "hashicorp/aws", version = "~> 5.0" }
    tls   = { source = "hashicorp/tls", version = "~> 4.0" }
    local = { source = "hashicorp/local", version = "~> 2.0" }
  }
}

provider "aws" {
  region  = var.region_east
  profile = var.profile
  default_tags { tags = var.tags }
}

provider "aws" {
  alias   = "west"
  region  = var.region_west
  profile = var.profile
  default_tags { tags = var.tags }
}

data "aws_caller_identity" "me" {}

locals {
  account    = data.aws_caller_identity.me.account_id
  bucket     = "cws-smoke-${local.account}-${var.artifact_bucket_region}"
  east_names = [for i in range(var.east_store_count) : "n${i + 1}"]
  west_names = [for i in range(var.west_store_count) : "n${var.east_store_count + i + 1}"]
  # name:host:raftport:clientport. host = the NODE NAME (a hostname), NOT the EIP: a non-numeric
  # host makes grpc-serve bind 0.0.0.0 (node-cluster.scm:355) — required on AWS, where an EIP is
  # 1:1 NAT'd and is NOT a local interface (binding it fails). Peers resolve the name via /etc/hosts
  # (hosts_block below) -> the peer's public EIP -> NAT -> the 0.0.0.0 listener.
  cluster_spec = join(",", [for n in concat(local.east_names, local.west_names) : "${n}:${n}:7000:2379"])
  # "<public-eip> <name>" for every node; cloud-init rewrites the node's OWN line to 127.0.0.1
  # (avoid EIP hairpin) and appends the rest to /etc/hosts.
  host_lines = concat(
    [for i, n in local.east_names : "${aws_eip.east_store[i].public_ip} ${n}"],
    [for i, n in local.west_names : "${aws_eip.west_store[i].public_ip} ${n}"],
  )
  hosts_block     = join("\n", local.host_lines)
  store_eip_cidrs = [for e in concat(aws_eip.east_store, aws_eip.west_store) : "${e.public_ip}/32"]
  all_node_cidrs  = concat(local.store_eip_cidrs, ["${aws_eip.apiserver.public_ip}/32"])
}

# ---- SSH key (self-contained) ------------------------------------------------
resource "tls_private_key" "ssh" { algorithm = "ED25519" }

resource "local_sensitive_file" "ssh_key" {
  content         = tls_private_key.ssh.private_key_openssh
  filename        = "${path.module}/cws-smoke.pem"
  file_permission = "0600"
}

resource "aws_key_pair" "east" {
  key_name   = "cws-smoke"
  public_key = tls_private_key.ssh.public_key_openssh
}

resource "aws_key_pair" "west" {
  provider   = aws.west
  key_name   = "cws-smoke"
  public_key = tls_private_key.ssh.public_key_openssh
}

# ---- default VPC / a subnet per region --------------------------------------
data "aws_vpc" "east" { default = true }
data "aws_subnets" "east" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.east.id]
  }
}
data "aws_vpc" "west" {
  provider = aws.west
  default  = true
}
data "aws_subnets" "west" {
  provider = aws.west
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.west.id]
  }
}

# ---- IAM: instances pull the artifact from S3 -------------------------------
resource "aws_iam_role" "node" {
  name = "cws-smoke-node"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "node_s3" {
  name = "cws-smoke-s3-read"
  role = aws_iam_role.node.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:GetObject", "s3:ListBucket"],
      Resource = ["arn:aws:s3:::${local.bucket}", "arn:aws:s3:::${local.bucket}/*"]
    }]
  })
}

resource "aws_iam_instance_profile" "node" {
  name = "cws-smoke-node"
  role = aws_iam_role.node.name
}

# ---- EIPs (allocate first so the cluster spec has deterministic IPs) ---------
resource "aws_eip" "east_store" {
  count  = var.east_store_count
  domain = "vpc"
  tags   = { Name = "cws-${local.east_names[count.index]}" }
}
resource "aws_eip" "west_store" {
  provider = aws.west
  count    = var.west_store_count
  domain   = "vpc"
  tags     = { Name = "cws-${local.west_names[count.index]}" }
}
resource "aws_eip" "apiserver" {
  domain = "vpc"
  tags   = { Name = "cws-apiserver" }
}

# ---- Security groups ---------------------------------------------------------
# Store SG: SSH from you; Raft(7000)+etcd-gRPC(2379) from every cluster node + apiserver.
resource "aws_security_group" "store_east" {
  name   = "cws-store-east"
  vpc_id = data.aws_vpc.east.id
  ingress {
    description = "ssh"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  ingress {
    description = "raft+etcd from cluster"
    from_port   = 2379
    to_port     = 7000
    protocol    = "tcp"
    cidr_blocks = local.all_node_cidrs
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
resource "aws_security_group" "store_west" {
  provider = aws.west
  name     = "cws-store-west"
  vpc_id   = data.aws_vpc.west.id
  ingress {
    description = "ssh"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  ingress {
    description = "raft+etcd from cluster"
    from_port   = 2379
    to_port     = 7000
    protocol    = "tcp"
    cidr_blocks = local.all_node_cidrs
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
# apiserver SG: SSH + 6443 from you; egress to the store nodes' 2379.
resource "aws_security_group" "apiserver" {
  name   = "cws-apiserver"
  vpc_id = data.aws_vpc.east.id
  ingress {
    description = "ssh"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  ingress {
    description = "kube-apiserver"
    from_port   = 6443
    to_port     = 6443
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---- store instances ---------------------------------------------------------
resource "aws_instance" "east_store" {
  count                  = var.east_store_count
  ami                    = var.ami_east
  instance_type          = var.instance_type
  key_name               = aws_key_pair.east.key_name
  subnet_id              = data.aws_subnets.east.ids[0]
  vpc_security_group_ids = [aws_security_group.store_east.id]
  iam_instance_profile   = aws_iam_instance_profile.node.name
  # re-run cloud-init when the bootstrap template changes (recreate the instance)
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/cloud-init-store.sh.tftpl", {
    node_name    = local.east_names[count.index]
    cluster_spec = local.cluster_spec
    hosts_block  = local.hosts_block
    bucket       = local.bucket
  })
  tags = { Name = "cws-${local.east_names[count.index]}" }
}
resource "aws_instance" "west_store" {
  provider               = aws.west
  count                  = var.west_store_count
  ami                    = var.ami_west
  instance_type          = var.instance_type
  key_name               = aws_key_pair.west.key_name
  subnet_id              = data.aws_subnets.west.ids[0]
  vpc_security_group_ids = [aws_security_group.store_west.id]
  iam_instance_profile   = aws_iam_instance_profile.node.name
  # re-run cloud-init when the bootstrap template changes (recreate the instance)
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/cloud-init-store.sh.tftpl", {
    node_name    = local.west_names[count.index]
    cluster_spec = local.cluster_spec
    hosts_block  = local.hosts_block
    bucket       = local.bucket
  })
  tags = { Name = "cws-${local.west_names[count.index]}" }
}

# ---- apiserver instance (provisioned by setup-apiserver.sh after apply) ------
resource "aws_instance" "apiserver" {
  ami                    = var.ami_east
  instance_type          = var.instance_type
  key_name               = aws_key_pair.east.key_name
  subnet_id              = data.aws_subnets.east.ids[0]
  vpc_security_group_ids = [aws_security_group.apiserver.id]
  user_data              = <<-EOF
    #!/usr/bin/env bash
    set -e
    apt-get update -qq
    apt-get install -y --no-install-recommends docker.io curl >/dev/null
    systemctl enable --now docker
    curl -sL "https://dl.k8s.io/release/v1.31.4/bin/linux/arm64/kubectl" -o /usr/local/bin/kubectl
    chmod +x /usr/local/bin/kubectl
  EOF
  tags                   = { Name = "cws-apiserver" }
}

# ---- EIP associations --------------------------------------------------------
resource "aws_eip_association" "east_store" {
  count         = var.east_store_count
  instance_id   = aws_instance.east_store[count.index].id
  allocation_id = aws_eip.east_store[count.index].id
}
resource "aws_eip_association" "west_store" {
  provider      = aws.west
  count         = var.west_store_count
  instance_id   = aws_instance.west_store[count.index].id
  allocation_id = aws_eip.west_store[count.index].id
}
resource "aws_eip_association" "apiserver" {
  instance_id   = aws_instance.apiserver.id
  allocation_id = aws_eip.apiserver.id
}
