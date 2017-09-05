using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;

namespace AzureStorage
{
    public abstract class ReloadingOnFailureDecoratorBase<TStorage>
    {
        protected abstract Func<Task<TStorage>> MakeStorage { get; }

        private readonly ReaderWriterLockSlim _sync = new ReaderWriterLockSlim();
        private Task<TStorage> _currentTask;

        private Task<TStorage> GetStorageAsync(bool reload = false)
        {
            bool CheckCurrentTask() => _currentTask != null && !(_currentTask.IsCompleted && reload);

            try
            {
                _sync.EnterReadLock();

                if (CheckCurrentTask())
                {
                    return _currentTask;
                }
            }
            finally
            {
                _sync.ExitReadLock();
            }

            try
            {
                _sync.EnterWriteLock();

                // double check
                if (CheckCurrentTask())
                {
                    return _currentTask;
                }

                return _currentTask = MakeStorage();
            }
            finally
            {
                _sync.ExitWriteLock();
            }
        }

        private bool CheckException(Exception ex)
        {
            if (ex is StorageException storageException)
            {
                var statusCode = (HttpStatusCode)storageException.RequestInformation.HttpStatusCode;
                return statusCode == HttpStatusCode.Forbidden;
            }

            return false;
        }

        protected void Wrap(Action<TStorage> func)
        {
            try
            {
                func(GetStorageAsync().Result);
                return;
            }
            catch (Exception ex)
            {
                if (!CheckException(ex))
                {
                    throw;
                }
            }

            func(GetStorageAsync(reload: true).Result);
        }

        protected T Wrap<T>(Func<TStorage, T> func)
        {
            try
            {
                return func(GetStorageAsync().Result);
            }
            catch (Exception ex)
            {
                if (!CheckException(ex))
                {
                    throw;
                }
            }

            return func(GetStorageAsync(reload: true).Result);
        }

        protected async Task WrapAsync(Func<TStorage, Task> func)
        {
            try
            {
                await func(await GetStorageAsync());
                return;
            }
            catch (Exception ex)
            {
                if (!CheckException(ex))
                {
                    throw;
                }
            }

            await func(await GetStorageAsync(reload: true));
        }

        protected async Task<T> WrapAsync<T>(Func<TStorage, Task<T>> func)
        {
            try
            {
                return await func(await GetStorageAsync());
            }
            catch (Exception ex)
            {
                if (!CheckException(ex))
                {
                    throw;
                }
            }

            return await func(await GetStorageAsync(reload: true));
        }
    }
}