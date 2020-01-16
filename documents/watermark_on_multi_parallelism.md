# watermark: 在多并行度下的watermark应用


代码链接：[SteamingWindowWatermarkCollectDelayDataExample](https://github.com/buildupchao/flink-examples/blob/master/src/main/java/com/buildupchao/flinkexamples/stream/SteamingWindowWatermarkCollectDelayDataExample.java)

我们通过如下代码设置了并行度为1：
```
StreamExecutionEnvironment#setParallelism(1);
```

如果不设置的话，代码在运行时会默认读取本地CPU数量设置并行度。

测试如下：
```
nc -lk 9999
```
输入如下数据：
```
test1,1579174589000
test2,1579174593000
test3,1579174599000
test4,1579174600000
test5,1579174601000
test6,1579174603000
test7,1579174604000
```
此时会发现window并没有被触发。
因为此时，这7条数据都是被不同的线程处理的。每个线程都有一个watermark。

因为在多并行度的情况下，watermark对齐会取所有channel最小的watermark，但是我们现在采用并行度，且这7条数据都被不同的线程所处理，到现在还没获取到最小的watermark，所以window服务被触发执行。

那么我们试一下把并行度调整为2，然后通过如下数据做测试：
```
test1,1538359890000
test2,1538359903000
test3,1538359908000
```
此时会发现前两条数据被触发了。