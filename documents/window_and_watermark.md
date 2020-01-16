# 对于"迟到（late element）"太多的数据，Flink怎么处理？

代码链接：[SteamingWindowWatermarkCollectDelayDataExample](https://github.com/buildupchao/flink-examples/blob/master/src/main/java/com/buildupchao/flinkexamples/stream/SteamingWindowWatermarkCollectDelayDataExample.java)

## 1.丢弃（默认）

```
nc -lk 9999
```
输入如下数据：
```
test1,1579174589000
test2,1579174599000
```
此时会触发一次window
```
test3,1579174589000
test4,1579174590000
test5,1579174591000
```
此时并没有触发window。因为输入的数据所在的窗口已经执行过了，Flink默认对这些迟到的数据采用丢弃的方式处理。

## 2.allowedLateness 指定允许数据延迟的时间

Flink提供了 `allowedLateness` 方法对迟到的数据设置一个延迟时间，在指定延迟时间内到达的数据还是可以出发window执行的。

例如，可以接收延迟两秒到达的数据。可以做如下设置：
```
DataStream#allowedLateness(Time.seconds(2))
```

做如下测试：
```
nc -lk 9999
```
输入如下数据：
```
test1,1579174589000
test2,1579174599000
```
此时会触发一次window
```
test3,1579174589000
test4,1579174590000
test5,1579174591000
```
每条数据都出发window执行
```
test6,1579174593000
```
再输入
```
test7,1579174589000
test8,1579174590000
test9,1579174591000
```
上述三行数据都出发了window的执行，继续
```
test10,1579174603000
```
再输入三行数据
```
test11,1579174589000
test12,1579174590000
test13,1579174591000
```
此时没有再触发window。

<strong style="color:red;">原因是什么？</strong>

## 3.