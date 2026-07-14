output "cluster_spec" {
  description = "The --cluster argument every node was started with"
  value       = local.cluster_spec
}

output "store_nodes" {
  description = "node name -> public IP"
  value = merge(
    { for i, n in local.east_names : n => aws_eip.east_store[i].public_ip },
    { for i, n in local.west_names : n => aws_eip.west_store[i].public_ip },
  )
}

# The apiserver dials this east store node's etcd-gRPC endpoint (local-quorum, fast commits).
output "etcd_endpoint" {
  value = "http://${aws_eip.east_store[0].public_ip}:2379"
}

output "apiserver_ip" {
  value = aws_eip.apiserver.public_ip
}

output "ssh_key" {
  value = "${path.module}/cws-smoke.pem"
}

output "ssh_apiserver" {
  value = "ssh -i ${path.module}/cws-smoke.pem ubuntu@${aws_eip.apiserver.public_ip}"
}

output "ssh_n1" {
  value = "ssh -i ${path.module}/cws-smoke.pem ubuntu@${aws_eip.east_store[0].public_ip}"
}
