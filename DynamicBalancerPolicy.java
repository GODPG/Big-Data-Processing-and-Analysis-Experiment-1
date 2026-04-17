package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.*;


 //核心公式: β_i <= β_c + Δ

public class DynamicBalancerPolicy extends BlockPlacementPolicyDefault {

    private NetworkTopology clusterMap;
    private static final double THRESHOLD_DELTA = 0.10;

    @Override
    public void initialize(Configuration conf, FSClusterStats stats,
                           NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) 
    {
        super.initialize(conf, stats, clusterMap, host2datanodeMap);
        this.clusterMap = clusterMap;
        System.err.println("=== [Paper-Mock] DynamicBalancerPolicy 初始化成功！ ===");
    }

    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
                                              Node writer, List<DatanodeStorageInfo> chosenNodes,
                                              boolean returnChosenNodes, Set<Node> excludedNodes,
                                              long blocksize, BlockStoragePolicy storagePolicy,
                                              EnumSet<AddBlockFlag> flags) {

        System.err.println("=== [Paper-Mock] 拦截到写入请求: " + srcPath + " ===");

        if (excludedNodes == null) 
        {
            excludedNodes = new HashSet<>();
        }

        try {
            List<Node> allNodes = clusterMap.getLeaves(NodeBase.ROOT);
            long totalCapacity = 0;
            long totalUsed = 0;

            for (Node node : allNodes) {
                if (node instanceof DatanodeDescriptor) {
                    DatanodeDescriptor dn = (DatanodeDescriptor) node;
                    totalCapacity += dn.getCapacity();
                    totalUsed += dn.getDfsUsed();
                }
            }

            double betaC = (totalCapacity > 0) ? ((double) totalUsed / totalCapacity) : 0.0;
            System.err.println("=== [Paper-Mock] 集群当前总体利用率 β_c: " + String.format("%.2f%%", betaC * 100) + " ===");


            for (Node node : allNodes)
            {
                if (node instanceof DatanodeDescriptor) 
                {
                    DatanodeDescriptor dn = (DatanodeDescriptor) node;
                    long nodeCap = dn.getCapacity();
                    long nodeUsed = dn.getDfsUsed();
                    double betaI = (nodeCap > 0) ? ((double) nodeUsed / nodeCap) : 0.0;

                    // 核心论文公式拦截：β_i > β_c + Δ
                    if (betaI > (betaC + THRESHOLD_DELTA))
                    {
                        System.err.println("=== [Paper-Mock] 拦截节点 " + dn.getIpAddr() + 
                                           " | 利用率: " + String.format("%.2f%%", betaI * 100) + 
                                           " > 阈值: " + String.format("%.2f%%", (betaC + THRESHOLD_DELTA) * 100) + 
                                           " | 动作: 已将其加入黑名单！ ===");
                        excludedNodes.add(dn);
                    } 
                    else 
                    {
                        System.err.println("=== [Paper-Mock] 节点 " + dn.getIpAddr() + 
                                           " | 利用率: " + String.format("%.2f%%", betaI * 100) + 
                                           " | 状态: 健康 ===");
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("=== [Paper-Mock] 动态利用率计算出错，回退到默认机制 ===");
        }

        System.err.println("=== [Paper-Mock] 负载过滤完毕，交由 Hadoop 底层机制分配剩余健康节点... ===");
        
        return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, 
                                  returnChosenNodes, excludedNodes, blocksize, storagePolicy, flags);
    }
}
