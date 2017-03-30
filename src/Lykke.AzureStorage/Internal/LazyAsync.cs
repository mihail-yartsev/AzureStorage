using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Lykke.AzureStorage.Internal
{
    internal sealed class AsyncLazy<T> : Lazy<Task<T>>
    {
        public AsyncLazy(Func<T> valueFactory, LazyThreadSafetyMode mode) :
            base(() => Task.Factory.StartNew(valueFactory), mode)
        { }
        public AsyncLazy(Func<Task<T>> taskFactory, LazyThreadSafetyMode mode) :
            base(() => Task.Factory.StartNew(() => taskFactory()).Unwrap())
        { }

        public TaskAwaiter<T> GetAwaiter() { return Value.GetAwaiter(); }
    }
}
