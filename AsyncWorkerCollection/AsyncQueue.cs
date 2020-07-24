﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace dotnetCampus.Threading
{
    /// <summary>
    /// 提供一个异步的队列。可以使用 await 关键字异步等待出队，当有元素入队的时候，等待就会完成。
    /// </summary>
    /// <typeparam name="T">存入异步队列中的元素类型。</typeparam>
    public class AsyncQueue<T> : IDisposable
    {
        private readonly SemaphoreSlim _semaphoreSlim;
        private readonly ConcurrentQueue<T> _queue;

        /// <summary>
        /// 创建一个 <see cref="AsyncQueue{T}"/> 的新实例。
        /// </summary>
        public AsyncQueue()
        {
            _semaphoreSlim = new SemaphoreSlim(0);
            _queue = new ConcurrentQueue<T>();
        }

        /// <summary>
        /// 获取此刻队列中剩余元素的个数。
        /// 请注意：因为线程安全问题，此值获取后值即过时，所以获取此值的代码需要自行处理线程安全。
        /// </summary>
        public int Count => _queue.Count;

        /// <summary>
        /// 入队。
        /// </summary>
        /// <param name="item">要入队的元素。</param>
        public void Enqueue(T item)
        {
            _queue.Enqueue(item);
            _semaphoreSlim.Release();
        }

        /// <summary>
        /// 将一组元素全部入队。
        /// </summary>
        /// <param name="source">要入队的元素序列。</param>
        public void EnqueueRange(IEnumerable<T> source)
        {
            var n = 0;
            foreach (var item in source)
            {
                _queue.Enqueue(item);
                n++;
            }
            _semaphoreSlim.Release(n);
        }

        /// <summary>
        /// 异步等待出队。当队列中有新的元素时，异步等待就会返回。
        /// </summary>
        /// <param name="cancellationToken">
        /// 你可以通过此 <see cref="CancellationToken"/> 来取消等待出队。
        /// 由于此方法有返回值，后续方法可能依赖于此返回值，所以如果取消将抛出 <see cref="TaskCanceledException"/>。
        /// </param>
        /// <returns>可以异步等待的队列返回的元素。</returns>
        public async Task<T> DequeueAsync(CancellationToken cancellationToken = default)
        {
            while (!_isDisposed)
            {
                await _semaphoreSlim.WaitAsync(cancellationToken).ConfigureAwait(false);

                if (_queue.TryDequeue(out var item))
                {
                    return item;
                }
            }

            return default;
        }

        /// <summary>
        /// 主要用来释放锁，让 DequeueAsync 方法返回，解决因为锁让此对象内存不释放
        /// </summary>
        public void Dispose()
        {
            // 当释放的时候，将通过 _queue 的 Clear 清空内容，而通过 _semaphoreSlim 的释放让 DequeueAsync 释放锁
            // 此时将会在 DequeueAsync 进入 TryDequeue 方法，也许此时依然有开发者在 _queue.Clear() 之后插入元素，但是没关系，我只是需要保证调用 Dispose 之后会让 DequeueAsync 方法返回而已
            _isDisposed = true;
            _queue.Clear();
            // 释放 DequeueAsync 方法
            _semaphoreSlim.Release(int.MaxValue);
            _semaphoreSlim.Dispose();
        }

        private bool _isDisposed;
    }
}
