using System;
using System.Threading.Tasks;
using Autofac;
using Common;

namespace AzureStorage.Queue
{
    public interface IQueueReader : IStartable, ITimerCommand
    {
        void RegisterPreHandler(Func<object, Task<bool>> preHandler);
        void RegisterHandler<T>(string id, Func<T, Task> handler);
        void RegisterErrorHandler<T>(string id, Func<T, Task> handler);
        string GetComponentName();
    }
}
