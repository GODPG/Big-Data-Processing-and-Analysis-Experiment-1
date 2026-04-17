# Big-Data-Processing-and-Analysis-Experiment-1


## 📂 核心源文件说明

本项目包含以下 5 个核心 Java 模块：

### 1. 高阶内核调优与论文复现模块
* **`MultiLayerHashPolicy.java` (选做一：一致性哈希多层副本放置策略)**
  * **功能：** 重写 HDFS 默认的机架感知策略（`BlockPlacementPolicyDefault`）。
  * **技术点：** 通过构建带有 100 个虚拟节点的双层 TreeMap 一致性哈希环，将数据块的随机分配改造为逻辑寻址，彻底解决集群扩容时产生的大规模数据重分布问题。

* **`DynamicBalancerPolicy.java` (选做二：基于磁盘利用率的动态平衡策略)**
  * **功能：** 论文复现代码。实现基于节点实时负载的动态副本调度。
  * **技术点：** 摒弃传统的静态拓扑分配，实时捕捉 NameNode 拓扑树的容量状态，计算集群平均利用率 $\beta_c$。通过数学判别式 $\beta_i > \beta_c + \Delta$ 精准识别超载节点，采用**“动态黑名单（Pre-filtering）”**机制对写入请求进行物理拦截，随后平滑交接给原生分配引擎。

* **`IterativeBalancerJob.java` (选做三：面向 MapReduce 的迭代式数据均衡分区)**
  * **功能：** 论文复现代码。解决极度倾斜数据集导致的 Reducer 内存溢出或长尾阻塞问题。
  * **技术点：** 引入 FGB（细粒度数据块）与 MP（微分区）缓存机制，在内存中执行降序贪心分配算法。最关键的突破是采用了**“Key 标签穿透（Key Tagging）”**技术，配合重写的 `DynamicRouterPartitioner`，强行接管了 Shuffle 阶段的无序 Hash 路由，实现了数据的精确打散与负载平衡。

### 2. Hadoop 基础扩展模块
* **`MergeDeduplicate.java`**(实验五熟悉常用的MapReduce操作第（1）题)
  * **功能：** 标准的 MapReduce 基础应用，实现多数据源的合并与精准去重，构建后续复杂分析的数据基座。
* **`MyFSDataInputStream.java`**(熟悉常用的HDFS操作第（2）题)
  * **功能：** 针对 HDFS 底层数据输入流的自定义扩展封装，为上层定制化读取提供 API 支持。
