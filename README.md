# Hadoop HDFS Kernel Tuning & MapReduce Load Balancing
> 基于 Hadoop 3.1.3 的 HDFS 内核级副本放置策略重构与 MapReduce 数据倾斜动态均衡实践。

本项目深入 Hadoop 底层架构，针对分布式存储中的“数据迁移风暴”与分布式计算中的“数据倾斜（Data Skew）”两大痛点，实施了源码级的拦截与重写。项目中包含两篇学术论文的算法级复现，通过**内核热注入（Class Shadowing）**与**动态路由穿透（Key Tagging）**等工程手段，在不破坏原生容灾机制的前提下，实现了集群负载的绝对均衡。

## 📂 核心源文件说明

本项目包含以下 5 个核心 Java 模块：

### 1. 高阶内核调优与论文复现模块
* **`MultiLayerHashPolicy.java` (选做一：一致性哈希多层副本放置策略)**
  * **功能：** 重写 HDFS 默认的机架感知策略（`BlockPlacementPolicyDefault`）。
  * **技术点：** 通过构建带有 100 个虚拟节点（Virtual Nodes）的双层 TreeMap 一致性哈希环，将数据块的“随机分配”改造为“逻辑寻址”，彻底解决集群扩容时产生的大规模数据重分布（搬家效应）问题。单机环境下采用“逻辑三副本、物理单实体”的解耦适配。

* **`DynamicBalancerPolicy.java` (选做二：基于磁盘利用率的动态平衡策略)**
  * **功能：** 论文复现代码。实现基于节点实时负载的动态副本调度。
  * **技术点：** 摒弃传统的静态拓扑分配，实时捕捉 NameNode 拓扑树的容量状态，计算集群平均利用率 $\beta_c$。通过数学判别式 $\beta_i > \beta_c + \Delta$ 精准识别超载节点，采用**“动态黑名单（Pre-filtering）”**机制对写入请求进行物理拦截，随后平滑交接给原生分配引擎。

* **`IterativeBalancerJob.java` (选做三：面向 MapReduce 的迭代式数据均衡分区)**
  * **功能：** 论文复现代码。解决极度倾斜数据集导致的 Reducer 内存溢出或长尾阻塞问题。
  * **技术点：** 引入 FGB（细粒度数据块）与 MP（微分区）缓存机制，在内存中执行降序贪心分配算法。最关键的突破是采用了**“Key 标签穿透（Key Tagging）”**技术，配合重写的 `DynamicRouterPartitioner`，强行接管了 Shuffle 阶段的无序 Hash 路由，实现了数据的精确打散与负载平衡。

### 2. Hadoop 基础扩展模块
* **`MergeDeduplicate.java`**
  * **功能：** 标准的 MapReduce 基础应用，实现多数据源的合并与精准去重，构建后续复杂分析的数据基座。
* **`MyFSDataInputStream.java`**
  * **功能：** 针对 HDFS 底层数据输入流的自定义扩展封装，为上层定制化读取提供 API 支持。

## 🚀 技术亮点 (Technical Highlights)

1. **源码级内核注入 (Kernel-Level Injection)**
   * 避开常规的应用层 API 调用，直接通过继承和重写 HDFS 块管理子系统（BlockManagement）的核心类。利用将自定义 jar 包物理注入 `HADOOP_CLASSPATH` 的方式，绕过双亲委派机制，实现对原生分配逻辑的“热替换”。
2. **算法复现与原生兼容的平衡**
   * 在复现论文的高级算法时，没有盲目重造轮子。例如在 `DynamicBalancerPolicy` 中，算法只负责剔除不健康节点，最终的容灾机架选择依然回调 `super.chooseTarget()`，完美兼顾了负载均衡与系统鲁棒性。
3. **计算层的动态路由接管**
   * 打破了 Mapper 无法干预 Shuffle 路由走向的系统限制，首创 `目标ID拼接 -> 分区器剥离 -> Reducer还原` 的闭环链路，将复杂的理论模型成功落地于受限的底层框架中。

## 🛠️ 编译与部署指南 (How to Run)

环境要求：`Hadoop 3.1.3` / `JDK 1.8`

### 1. HDFS 内核策略注入 (以 DynamicBalancerPolicy 为例)
```bash
# 1. 编译自定义策略类
javac -cp $(hadoop classpath) -d . DynamicBalancerPolicy.java

# 2. 打包为 Jar
jar -cvf custom-hdfs-policy.jar org/

# 3. 物理覆盖与环境变量配置
cp custom-hdfs-policy.jar /usr/local/hadoop/share/hadoop/common/lib/
export HADOOP_CLASSPATH=/usr/local/hadoop/share/hadoop/common/lib/custom-hdfs-policy.jar

# 4. 重启 HDFS 以加载内核新逻辑
stop-dfs.sh
start-dfs.sh
