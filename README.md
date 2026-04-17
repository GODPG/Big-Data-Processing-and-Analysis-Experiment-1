# Big-Data-Processing-and-Analysis-Experiment-1


## 📂 核心源文件说明

本项目包含以下 5 个核心 Java 源码：


### 1. HDFS与MapReduce练习
* **`MergeDeduplicate.java`**(实验五熟悉常用的MapReduce操作第（1）题)
  * **功能：** 标准的 MapReduce 基础应用，实现多数据源的合并与精准去重。
* **`MyFSDataInputStream.java`**(熟悉常用的HDFS操作第（2）题)
  * **功能：** 针对 HDFS 底层数据输入流的自定义扩展封装，为上层定制化读取提供 API 支持。

### 2. 论文复现
* **`MultiLayerHashPolicy.java` (选做一：一致性哈希多层副本放置策略)**
  * **功能：** 重写 HDFS 默认的机架感知策略（`BlockPlacementPolicyDefault`）。

* **`DynamicBalancerPolicy.java` (选做二：基于磁盘利用率的动态平衡策略)**
  * **功能：** 论文复现代码。实现基于节点实时负载的动态副本调度。

* **`IterativeBalancerJob.java` (选做三：面向 MapReduce 的迭代式数据均衡分区)**
  * **功能：** 论文复现代码。解决极度倾斜数据集导致的 Reducer 内存溢出或长尾阻塞问题。

