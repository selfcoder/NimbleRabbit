using System;
using System.Threading;

namespace NimbleRabbit.Tests
{
    public static class CancellationTokenHelper
    {
        public static CancellationToken TimeoutSeconds(int seconds)
        {
            return new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;
        }
    }
}