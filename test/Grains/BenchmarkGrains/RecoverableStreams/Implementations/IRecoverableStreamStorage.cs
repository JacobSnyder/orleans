using System;
using System.Threading.Tasks;
using BenchmarkGrains.RecoverableStreams;

namespace Orleans.Streams
{
    internal interface IRecoverableStreamStorage<TState>
    {
        RecoverableStreamState<TState> State { get; set; }

        TimeSpan TimerPeriod { get; }

        Task Load();

        Task<bool> Save();

        Task<(bool persisted, bool fastForwardRequested)> CheckpointIfOverdue();
    }
}