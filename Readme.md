## todo 
1. checkSubmitJobs:如何产生任务？依旧是模拟器？
2. 输入界面，调度界面，调度结果界面
3. checkFinishedJobs: 如何通信？ channel？
4. 物理资源的收集: 配置文件实现
5. 任务的完成：
   1. pod完成的事件多次检测到，如何处理？
   2. 现在是启动一个pod模拟，那么模拟运行时运行时间应该是多少如何指定？
6. 任务的调度：
   1. 目前是启动一个pod模拟→转换为k8s真正的调度 deployment

## 2022/4/7
1. 任务是发送个apiserver，从apiserver中获取任务。
2. 任务的使用是创建deployment,应该使用job?
3. err的处理，终止线程
4. 持有并等待，没有持有，如何等待？