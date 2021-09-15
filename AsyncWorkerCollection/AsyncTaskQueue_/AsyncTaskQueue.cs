using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;

namespace dotnetCampus.Threading
{
    /// <summary>
    /// 异步任务队列，将任务加入到队列里面按照顺序执行
    /// </summary>
#if PublicAsInternal
    internal
#else
    public
#endif
    class AsyncTaskQueue : IDisposable
    {
        /// <summary>
        /// 异步任务队列
        /// </summary>
        public AsyncTaskQueue()
        {
            _autoResetEvent = new AsyncAutoResetEvent(false);
            InternalRunning();
        }

        #region 执行

        /// <summary>
        /// 执行异步操作
        /// </summary>
        /// <typeparam name="T">返回结果类型</typeparam>
        /// <param name="func">异步操作</param>
        /// <returns>IsInvalid:异步操作是否有效(多任务时，如果设置了<see cref="AutoCancelPreviousTask"/>，只会保留最后一个任务有效)；Result:异步操作结果</returns>
        public async Task<(bool IsInvalid, T Result)> ExecuteAsync<T>(Func<Task<T>> func)
        {
            var task = GetExecutableTask(func);
            var result = await await task;
            if (!task.IsValid)
            {
                result = default;
            }

            return (task.IsValid, result);
        }

        /// <summary>
        /// 执行异步操作
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        // ReSharper disable once UnusedTypeParameter
        public async Task<bool> ExecuteAsync<T>(Func<Task> func)
        {
            var task = GetExecutableTask(func);
            await await task;
            return task.IsValid;
        }

        #endregion

        #region 添加任务

        /// <summary>
        /// 获取待执行任务
        /// </summary>
        /// <param name="action"></param>
        /// <returns></returns>
        private AwaitableTask GetExecutableTask(Action action)
        {
            var awaitableTask = new AwaitableTask(new Task(action));
            AddPendingTaskToQueue(awaitableTask);
            return awaitableTask;
        }

        /// <summary>
        /// 获取待执行任务
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="function"></param>
        /// <returns></returns>
        private AwaitableTask<TResult> GetExecutableTask<TResult>(Func<TResult> function)
        {
            var awaitableTask = new AwaitableTask<TResult>(new Task<TResult>(function));
            AddPendingTaskToQueue(awaitableTask);
            return awaitableTask;
        }

        /// <summary>
        /// 添加待执行任务到队列
        /// </summary>
        /// <param name="task"></param>
        /// <returns></returns>
        private void AddPendingTaskToQueue(AwaitableTask task)
        {
            //添加队列，加锁。
            lock (Locker)
            {
                if (_isDisposing || _isDisposed)
                {
                    task.SetNotExecutable();
                    return;
                }

                _queue.Enqueue(task);
                //开始执行任务
                _autoResetEvent.Set();
            }
        }

        #endregion

        #region 内部运行

        private async void InternalRunning()
        {
            while (!_isDisposing)
            {
                Task<bool> waitOneTask = null;
                bool shouldWaitOneTask = _queue.Count == 0;

                lock (Locker)
                {
                    if (_isDisposing || _isDisposed)
                    {
                        // 理论上判断 _isDisposed 为 true 时，此时 _isDisposing 一定为 true 的值
                        // 但是如果是在 _autoResetEvent 被释放时，由此线程进入到此判断里面
                        // 那么 _isDisposing 为 true 但 _isDisposed 为 false 的值
                        // 原因在于 _isDisposed 等待 _autoResetEvent 释放之后，再设置为 true 的值
                        // 同时因为进入的线程是在释放 _autoResetEvent 的线程，因此 lock (Locker) 无效
                        return; 
                    }

                    // 在锁里获取异步锁，这样可以解决在释放的时候，调用异步锁已被释放
                    if (shouldWaitOneTask)
                    {
                        waitOneTask = _autoResetEvent.WaitOneAsync();
                    }
                }

                if (shouldWaitOneTask)
                {
                    //等待后续任务
                   var result = await waitOneTask.ConfigureAwait(false);

                   if (result is false)
                   {
                        // 啥时候是 false 的值？在 _autoResetEvent 被释放的时候
                        // 此时将会因为其他线程调用 _autoResetEvent 的 Dispose 方法而继续往下走
                        // 此时在 Dispose 方法里面是获得了 Locker 这个对象的锁。也就是说此时如果判断 _isDisposed 属性，是一定是 false 的值。原因是 _isDisposed 的设置是在 Locker 锁里面，同时也在 _autoResetEvent 被释放之后。尽管有在外层的 while (!_isDisposing) 进行一次判断，然而此获取非线程安全。因此需要进行三步判断才能是安全的
                        // 第一步是最外层的 while (!_isDisposing) 进行判断。第二步是进入 Locker 锁时，同时判断 _isDisposing 和 _isDisposed 对象（其实判断 _isDisposing 即可）不过多余的判断没有什么锅
                        // 第三步是此分支，如果当前释放了，那就应该返回了
                        return;
                   }
                }

                while (TryGetNextTask(out var task))
                {
                    //如已从队列中删除
                    if (!task.Executable) continue;
                    //添加是否已释放的判断
                    if (!_isDisposing)
                    {
                        if (UseSingleThread)
                        {
                            task.RunSynchronously();
                        }
                        else
                        {
                            task.Start();
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 上一次异步操作
        /// </summary>
        private AwaitableTask _lastDoingTask;

        private bool TryGetNextTask(out AwaitableTask task)
        {
            task = null;
            while (_queue.Count > 0)
            {
                //获取并从队列中移除任务
                if (_queue.TryDequeue(out task) && (!AutoCancelPreviousTask || _queue.Count == 0))
                {
                    //设置进行中的异步操作无效
                    _lastDoingTask?.MarkTaskInvalid();
                    _lastDoingTask = task;
                    return true;
                }

                Debug.Assert(task != null);
                //并发操作，设置任务不可执行
                task.SetNotExecutable();
            }

            return false;
        }

        #endregion

        #region dispose

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// 析构任务队列
        /// </summary>
        ~AsyncTaskQueue()
        {
            Dispose(false);
        }

        private void Dispose(bool disposing)
        {
            if (_isDisposed) return;

            lock (Locker)
            {
                if (_isDisposed) return;
                _isDisposing = true;
                if (disposing)
                {
                }

                // 先调用 Clear 方法，然后调用  _autoResetEvent.Dispose 此时的任务如果还没执行的，就不会执行
                _queue.Clear();
                _autoResetEvent.Dispose();
                _isDisposed = true;
            }
        }

        #endregion

        #region 属性及字段

        /// <summary>
        /// 是否使用单线程完成任务
        /// </summary>
        public bool UseSingleThread { get; set; } = true;

        /// <summary>
        /// 自动取消以前的任务，此属性应该是在创建对象完成之后给定，不允许在任务执行过程中更改
        /// </summary>
        /// 设置和获取不需要加上锁，因为这是原子的，业务上也不会有开发者不断修改这个值。也就是说这个属性只有在对象创建就给定
        public bool AutoCancelPreviousTask
        {
            get => _autoCancelPreviousTask;
            set
            {
                if (_lastDoingTask != null)
                {
                    // 仅用于开发时告诉开发者，在任务开始之后调用是不对的
                    throw new InvalidOperationException($"此属性应该是在创建对象完成之后给定，不允许在任务执行过程中更改");
                }

                _autoCancelPreviousTask = value;
            }
        }

        private object Locker => _queue;
        private bool _isDisposed;
        private bool _isDisposing;
        private readonly ConcurrentQueue<AwaitableTask> _queue = new ConcurrentQueue<AwaitableTask>();
        private readonly AsyncAutoResetEvent _autoResetEvent;
        // ReSharper disable once RedundantDefaultMemberInitializer
        private bool _autoCancelPreviousTask = false;

        #endregion
    }
}
