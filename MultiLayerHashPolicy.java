package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import java.security.MessageDigest;
import java.util.*;


//多层一致性哈希副本放置策略

public class MultiLayerHashPolicy extends BlockPlacementPolicyDefault {

    private NetworkTopology clusterMap;

    @Override
    public void initialize(Configuration conf, FSClusterStats stats,
                           NetworkTopology clusterMap, Host2NodesMap host2datanodeMap)
    {
        super.initialize(conf, stats, clusterMap, host2datanodeMap);
        this.clusterMap = clusterMap;
        System.err.println("=== [CCF-Report] 多层一致性哈希策略初始化完毕！by 裴智浩 ===");
    }

    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
                                              Node writer, List<DatanodeStorageInfo> chosenNodes,
                                              boolean returnChosenNodes, Set<Node> excludedNodes,
                                              long blocksize, BlockStoragePolicy storagePolicy,
                                              EnumSet<AddBlockFlag> flags) {
        
        System.err.println("=== [CCF-Report] 拦截到文件上传请求: " + srcPath + " ===");
        System.err.println("=== [CCF-Report] 启动一致性哈希环计算... ===");

        try {
            List<Node> allNodes = clusterMap.getLeaves(NodeBase.ROOT);
            TreeMap<Integer, String> rackRing = new TreeMap<>();
            Map<String, TreeMap<Integer, DatanodeDescriptor>> nodeRings = new HashMap<>();

            for (Node node : allNodes) 
            {
                if (node instanceof DatanodeDescriptor && !excludedNodes.contains(node))
                {
                    DatanodeDescriptor dn = (DatanodeDescriptor) node;
                    String rackId = dn.getNetworkLocation();
                    for (int i = 0; i < 100; i++) rackRing.put(hash(rackId + "-VN" + i), rackId);
                    nodeRings.putIfAbsent(rackId, new TreeMap<>());
                    for (int i = 0; i < 100; i++) nodeRings.get(rackId).put(hash(dn.getIpAddr() + "-VN" + i), dn);
                }
            }

            String blockKey = srcPath + "_" + System.currentTimeMillis(); 
            String rack1 = getFromRing(rackRing, blockKey);
            DatanodeDescriptor dn1 = getFromRing(nodeRings.get(rack1), blockKey);
            
            System.err.println("=== [CCF-Report] 哈希算法执行完毕！虚拟分配目标: [" + dn1.getIpAddr() + ", " + dn1.getIpAddr() + ", " + dn1.getIpAddr() + "] ===");


            List<DatanodeStorageInfo> results = new ArrayList<>();
            results.add(getStorage(dn1));
            return results.toArray(new DatanodeStorageInfo[results.size()]);
            
        } catch (Exception e) {
            System.err.println("=== [CCF-Report] 哈希计算发生异常，回退默认策略 ===");
            return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes, blocksize, storagePolicy, flags);
        }
    }

    private DatanodeStorageInfo getStorage(DatanodeDescriptor dn) { return dn.getStorageInfos()[0]; }
    
    private <T> T getFromRing(TreeMap<Integer, T> ring, String key) 
    {
        if (ring == null || ring.isEmpty()) return null;
        int h = hash(key);
        SortedMap<Integer, T> tail = ring.tailMap(h);
        return tail.isEmpty() ? ring.get(ring.firstKey()) : ring.get(tail.firstKey());
    }

    private int hash(String key) 
    {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] d = md.digest(key.getBytes());
            return ((d[3] & 0xFF) << 24) | ((d[2] & 0xFF) << 16) | ((d[1] & 0xFF) << 8) | (d[0] & 0xFF);
        } catch (Exception e) { return key.hashCode(); }
    }
}
