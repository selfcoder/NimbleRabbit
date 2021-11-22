using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NimbleRabbit
{
    public interface ITaskDispatcher
    {
        event UnhandledExceptionEventHandler UnhandledException;
        bool TrySchedule(Func<Task> action);
        Task GetCompletionTask();
        void Stop();
    }

    public class TaskDispatcher : ITaskDispatcher, IDisposable
    {
        public event UnhandledExceptionEventHandler UnhandledException;

        private readonly BlockingCollection<Func<Task>> _queue;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly IList<Task> _threadTasks;

        private int _maxThreadCount;
        private int _maxTaskPerThreadCount;
        private long _runningThreadCount;

        public TaskDispatcher()
        {
            _queue = new BlockingCollection<Func<Task>>();
            _cancellationTokenSource = new CancellationTokenSource();
            _threadTasks = new List<Task>();
            _maxThreadCount = Environment.ProcessorCount;
            _maxTaskPerThreadCount = 5;
            _runningThreadCount = 1;
            StartNewThread(0);
        }

        public int MaxThreads
        {
            get => _maxThreadCount;
            set
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException();
                _maxThreadCount = value;
            }
        }

        public int MaxTasksPerThread
        {
            get => _maxTaskPerThreadCount;
            set
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException();
                _maxTaskPerThreadCount = value;
            }
        }

        public bool TrySchedule(Func<Task> action)
        {
            try
            {
                return _queue.TryAdd(action, Timeout.Infinite, _cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }
        
        public void Stop()
        {
            if (_cancellationTokenSource.IsCancellationRequested)
                return;

            _queue.CompleteAdding();
            _cancellationTokenSource.Cancel();
        }

        public Task GetCompletionTask()
        {
            Task[] tasks;
            lock (_threadTasks)
            {
                tasks = _threadTasks.ToArray();
            }

            return Task.WhenAll(tasks);
        }

        public void Dispose()
        {
            _queue.Dispose();
            _cancellationTokenSource.Dispose();
        }

        private void StartNewThread(int index)
        {
            var task = Task.Factory.StartNew(ThreadWorker, index, _cancellationTokenSource.Token,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
            lock (_threadTasks)
            {
                _threadTasks.Add(task);
            }

            task.ContinueWith(t =>
            {
                lock (_threadTasks)
                {
                    _threadTasks.Remove(t);
                }
            });
        }

        private void TryStartNewThread()
        {
            long currentThreadCount = Interlocked.Read(ref _runningThreadCount);
            if (currentThreadCount < _maxThreadCount &&
                Interlocked.CompareExchange(ref _runningThreadCount, currentThreadCount + 1, currentThreadCount) == currentThreadCount)
            {
                StartNewThread((int)currentThreadCount);
            }
        }

        private async Task ThreadWorker(object state)
        {
            try
            {
                int workerIndex = (int)state;
                var tasks = new List<Task>(_maxTaskPerThreadCount);
                while (!_queue.IsCompleted)
                {
                    int maxCount = _maxTaskPerThreadCount;
                    int index;
                    for (index = 0; index < maxCount; index++)
                    {
                        if (_queue.IsAddingCompleted)
                            break;

                        if (!_queue.TryTake(
                            out var action,
                            index == 0 && workerIndex == 0 ? -1 : 10,
                            _cancellationTokenSource.Token))
                        {
                            break;
                        }

                        Task task;
                        try
                        {
                            task = action();
                        }
                        catch (Exception exception)
                        {
                            OnException(exception);
                            continue;
                        }

                        tasks.Add(task);
                    }

                    if (tasks.Count == 0)
                    {
                        if (_queue.IsAddingCompleted || workerIndex > 0)
                        {
                            break;
                        }
                    }
                    else if (tasks.Count == maxCount && !_queue.IsAddingCompleted)
                    {
                        TryStartNewThread();
                    }

                    try
                    {
                        await Task.WhenAll(tasks);
                    }
                    catch (Exception exception)
                    {
                        OnException(exception);
                    }

                    if (workerIndex >= _maxThreadCount)
                    {
                        break;
                    }

                    tasks.Clear();
                }
            }
            finally
            {
                Interlocked.Decrement(ref _runningThreadCount);
            }
        }

        private void OnException(Exception exception)
        {
            UnhandledException?.Invoke(this, new UnhandledExceptionEventArgs(exception, false));
        }
    }
}