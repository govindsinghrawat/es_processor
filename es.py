import requests
from typing import List
from dataclasses import dataclass

@dataclass
class EsCluster:
    clusterName: str
    clusterEntryPoint: str
    clusterId: int
    computeName: str
    computeEntryPoint: str
    computeId: int
    nodes: List['ComputeNode'] = None

@dataclass
class EsNode:
    esNodeName: str
    ipv4: str
    ipv6: str
    diskSize: int
    computeNode: 'ComputeNode' = None

@dataclass
class ComputeNode:
    computePodName: str
    computeCluster: str
    ipv4: str
    ipv6: str
    nfsCluster: str
    esNode: 'EsNode' = None

class RegionProcessor:
    def __init__(self, regionUrl: str):
        self.regionUrl = regionUrl
        self.esClusters = []

    def process_region_data(self, data: List[dict]) -> None:
        for cluster_data in data:
            es_cluster = self.process_cluster_data(cluster_data)
            self.esClusters.append(es_cluster)

    def process_cluster_data(self, data: dict) -> EsCluster:
        # Create EsCluster object and update its values
        es_cluster = EsCluster(data['clusterName'], data['clusterEntryPoint'], data['clusterId'],
                               data['computeName'], data['computeEntryPoint'], data['computeId'])

        # Invoke HTTP GET requests to retrieve JSON responses
        compute_nodes_json = self.get_json_response(es_cluster.computeEntryPoint)
        es_nodes_json = self.get_json_response(es_cluster.clusterEntryPoint)

        # Build nodes list for EsCluster
        es_cluster.nodes = []
        for compute_node_data in compute_nodes_json:
            es_node_name = self.find_es_node(compute_node_data['esNodeName'], es_nodes_json)
            es_node = EsNode(es_node_name, compute_node_data['ipv4'], compute_node_data['ipv6'],
                             compute_node_data['diskSize'])  # No need to assign computeNode here
            compute_node = ComputeNode(compute_node_data['computePodName'], compute_node_data['computeCluster'],
                                        compute_node_data['ipv4'], compute_node_data['ipv6'],
                                        compute_node_data['nfsCluster'], es_node)
            es_node.computeNode = compute_node  # Assigning ComputeNode reference to EsNode
            es_cluster.nodes.append(compute_node)

        return es_cluster

    def get_json_response(self, url: str) -> dict:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to retrieve JSON response from {url}")

    def find_es_node(self, es_node_name: str, es_nodes_json: List[dict]) -> str:
        for es_node_data in es_nodes_json:
            if es_node_data['esNodeName'] == es_node_name:
                return es_node_data['esNodeName']
        return None  # Return None if corresponding EsNode is not found

# Example usage:
region_data = [
    {
        'clusterName': 'MyESCluster',
        'clusterEntryPoint': 'http://es-cluster:9200',
        'clusterId': 12345,
        'computeName': 'MyComputeCluster',
        'computeEntryPoint': 'http://compute-cluster:8080',
        'computeId': 67890
    },
    {
        'clusterName': 'MyESCluster2',
        'clusterEntryPoint': 'http://es-cluster:2200',
        'clusterId': 12342,
        'computeName': 'MyComputeCluster2',
        'computeEntryPoint': 'http://compute-cluster:8082',
        'computeId': 67892
    }
]

region_processor = RegionProcessor('http://region-url')
region_processor.process_region_data(region_data)

# Accessing attributes of EsClusters in the region
for idx, es_cluster in enumerate(region_processor.esClusters):
    print(f"\nEsCluster {idx + 1}:")
    print("Cluster Name:", es_cluster.clusterName)
    print("Cluster Entry Point:", es_cluster.clusterEntryPoint)
    print("Cluster ID:", es_cluster.clusterId)
    print("Compute Name:", es_cluster.computeName)
    print("Compute Entry Point:", es_cluster.computeEntryPoint)
    print("Compute ID:", es_cluster.computeId)
    print("Nodes:")
    for node_idx, node in enumerate(es_cluster.nodes):
        print(f"\tNode {node_idx + 1}:")
        print("\tCompute Pod Name:", node.computePodName)
        print("\tCompute Cluster:", node.computeCluster)
        print("\tIPv4:", node.ipv4)
        print("\tIPv6:", node.ipv6)
        print("\tNFS Cluster:", node.nfsCluster)
        print("\tAssociated EsNode Name:", node.esNode.esNodeName)
        print("\tAssociated EsNode IPv4:", node.esNode.ipv4)
        print("\tAssociated EsNode IPv6:", node.esNode.ipv6)
        print("\tAssociated EsNode Disk Size:", node.esNode.diskSize)
