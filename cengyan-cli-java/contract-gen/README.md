# 契约用例生成

这里的脚本不参与 Maven 构建，只在服务端分片算法或 pycengyan 错误码映射变更后手动重跑，
把结果提交到 `src/test/resources/contract/`。SDK 构建本身不依赖服务端模块或 Python。

在 `cengyan-cli-java` 目录下执行：

```bash
# 分片哈希向量：用服务端 io.cengyan.protocol.PartitionRouter / EntityId 生成（需要 JDK 11+ 直接运行单文件源码）
java -cp ../cengyan-protocol/target/cengyan-protocol-1.0.0.jar \
     contract-gen/GeneratePartitionVectors.java src/test/resources/contract/partition-vectors.json

# 错误码映射：从 pycengyan 的 exceptions 生成
python -I contract-gen/generate_error_codes.py src/test/resources/contract/error-codes.json ../cengyan-cli-python/src
```

重新生成后跑 `mvn test`，`ContractTest` 会逐条校验 Java SDK。
