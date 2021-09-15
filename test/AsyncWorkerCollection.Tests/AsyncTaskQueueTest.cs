using System;
using System.Threading;
using System.Threading.Tasks;
using dotnetCampus.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MSTest.Extensions.Contracts;

namespace AsyncWorkerCollection.Tests
{
    [TestClass]
    public class AsyncTaskQueueTest
    {
        [ContractTestCase]
        public void DisposeTest()
        {
            "在执行任务时先加入新的任务再调用清理，可以成功清理".Test(async () =>
            {
                AsyncTaskQueue asyncTaskQueue =
                    new AsyncTaskQueue() { AutoCancelPreviousTask = true, UseSingleThread = true };

                var task = asyncTaskQueue.ExecuteAsync(() =>
                {
                    _ = asyncTaskQueue.ExecuteAsync<bool>((Func<Task>) (() => Task.CompletedTask));

                    asyncTaskQueue.Dispose();

                    return Task.FromResult(1);
                });

                var result = await task;
                // 没有抛出异常就是符合预期
                // 因为在清理的时候清空队列，因此第一个任务能成功
                Assert.AreEqual(true, result.IsInvalid);
                Assert.AreEqual(1, result.Result);
            });

            "在执行任务时先调用清理再加入新的任务，可以成功清理".Test(async () =>
            {
                AsyncTaskQueue asyncTaskQueue =
                    new AsyncTaskQueue() {AutoCancelPreviousTask = true, UseSingleThread = true};

                var task = asyncTaskQueue.ExecuteAsync(() =>
                {
                    // 先清理再加入任务
                    asyncTaskQueue.Dispose();
                    _ = asyncTaskQueue.ExecuteAsync<bool>((Func<Task>) (() => Task.CompletedTask));

                    return Task.FromResult(1);
                });

                var result = await task;
                // 没有抛出异常就是符合预期
                // 被清理之后加入的任务将啥都不做，因此第一个任务可以成功
                Assert.AreEqual(true, result.IsInvalid);
                Assert.AreEqual(1, result.Result);
            });

            "在执行任务时，调用清理 AsyncTaskQueue 的逻辑，可以成功清理".Test(async () =>
            {
                AsyncTaskQueue asyncTaskQueue = new AsyncTaskQueue() { AutoCancelPreviousTask = true, UseSingleThread = true };

                var autoResetEvent1 = new AsyncAutoResetEvent(false);
                var autoResetEvent2 = new AsyncAutoResetEvent(false);

                var task = asyncTaskQueue.ExecuteAsync(async () =>
                {
                    // 让第二个任务加入
                    autoResetEvent1.Set();

                    // 等待第二个任务加入完成
                    await autoResetEvent2.WaitOneAsync();

                    asyncTaskQueue.Dispose();
                    return 1;
                });

                await autoResetEvent1.WaitOneAsync();
                _ = asyncTaskQueue.ExecuteAsync<bool>((Func<Task>) (() => Task.CompletedTask));
                autoResetEvent2.Set();

                var result = await task;
                // 没有抛出异常就是符合预期
                // 因为第二个任务的加入而让第一个任务失败
                Assert.AreEqual(false, result.IsInvalid);
            });

            "连续开始任务的时候，无法立刻清理".Test(async () =>
            {
                AsyncTaskQueue asyncTaskQueue = new AsyncTaskQueue() { AutoCancelPreviousTask = true, UseSingleThread = true };

                var autoResetEvent = new AutoResetEvent(false);
                _ = Task.Run(() =>
                {
                    autoResetEvent.WaitOne();
                    asyncTaskQueue.Dispose();
                });

                var result = await asyncTaskQueue.ExecuteAsync(async () =>
                {
                    await Task.Delay(10);
                    autoResetEvent.Set();
                    return 1;
                });

                await Task.Delay(100);

                for (int i = 0; i < 10; i++)
                {
                   _ = asyncTaskQueue.ExecuteAsync<bool>((Func<Task>)(() => Task.CompletedTask));
                }

                await Task.Delay(1000);
                autoResetEvent.Set();

                // 没有抛出异常就是符合预期
                Assert.AreEqual(true, result.IsInvalid);
                Assert.AreEqual(1, result.Result);
            });

            "在执行任务结束的时候调用 AsyncTaskQueue 销毁方法，可以不抛异常销毁".Test(async () =>
            {
                AsyncTaskQueue asyncTaskQueue = new AsyncTaskQueue() { AutoCancelPreviousTask = true, UseSingleThread = true };
                var autoResetEvent = new AutoResetEvent(false);
                _ = Task.Run(() =>
                {
                    autoResetEvent.WaitOne();
                    asyncTaskQueue.Dispose();
                });

                var result = await asyncTaskQueue.ExecuteAsync(async () =>
                {
                    await Task.Delay(10);
                    autoResetEvent.Set();
                    return 1;
                });

                Thread.Sleep(2000);

                Assert.AreEqual(true, result.IsInvalid);
                Assert.AreEqual(1, result.Result);
            });

            "调用 AsyncTaskQueue 销毁方法，可以不抛异常销毁".Test(async () =>
            {
                AsyncTaskQueue asyncTaskQueue = new AsyncTaskQueue() { AutoCancelPreviousTask = true, UseSingleThread = true };

                var result = await asyncTaskQueue.ExecuteAsync(async () =>
                {
                    await Task.Delay(10);
                    return 0;
                });

                asyncTaskQueue.Dispose();

                Thread.Sleep(2000);

                Assert.AreEqual(true, result.IsInvalid);
                Assert.AreEqual(0, result.Result);
            });
        }
    }
}
