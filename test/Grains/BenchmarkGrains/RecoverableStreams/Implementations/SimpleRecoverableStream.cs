using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public interface IStreamProviderNameResolver
    {
        string GetStreamProviderName(string streamNamespace);
    }

    public interface IOrleansHooks<TEvent>
    {
        Task SubscribeAsync(
            IStreamIdentity streamId,
            Func<TEvent, StreamSequenceToken, Task> onNextAsync,
            Func<Exception, Task> onErrorAsync,
            StreamSequenceToken token);

        void DeactivateOnIdle();
    }

    public class OrleansHooks<TEvent> : IOrleansHooks<TEvent>
    {
        public delegate IStreamProvider StreamProviderFactory(string name);

        private readonly IStreamProviderNameResolver streamProviderNameResolver;
        private readonly StreamProviderFactory streamProviderFactory;
        private readonly Action deactivateOnIdle;

        public OrleansHooks(
            IStreamProviderNameResolver streamProviderNameResolver,
            StreamProviderFactory streamProviderFactory,
            Action deactivateOnIdle)
        {
            if (streamProviderNameResolver == null) { throw new ArgumentNullException(nameof(streamProviderNameResolver)); }
            if (streamProviderFactory == null) { throw new ArgumentNullException(nameof(streamProviderFactory)); }
            if (deactivateOnIdle == null) { throw new ArgumentNullException(nameof(deactivateOnIdle)); }

            this.streamProviderNameResolver = streamProviderNameResolver;
            this.streamProviderFactory = streamProviderFactory;
            this.deactivateOnIdle = deactivateOnIdle;
        }

        public async Task SubscribeAsync(IStreamIdentity streamId, Func<TEvent, StreamSequenceToken, Task> onNextAsync, Func<Exception, Task> onErrorAsync, StreamSequenceToken token)
        {
            // Get the Stream Provider Name (also known as the Stream Provider ID)
            var streamProviderName = this.streamProviderNameResolver.GetStreamProviderName(streamId.Namespace);
            
            var streamProvider = this.streamProviderFactory.Invoke(streamProviderName);

            var stream = streamProvider.GetStream<TEvent>(streamId.Guid, streamId.Namespace);

            if (!stream.IsRewindable)
            {
                throw new NotSupportedException(FormattableString.Invariant($"Stream must be Rewindable"));
            }

            var handles = await stream.GetAllSubscriptionHandles();

            if (handles.Count == 0)
            {
                await stream.SubscribeAsync(onNextAsync, onErrorAsync, token);
            }
            else
            {
                if (handles.Count > 1)
                {
                    // TODO: Warn
                }

                await handles[0].ResumeAsync(onNextAsync, onErrorAsync, token);
            }
        }

        public void DeactivateOnIdle()
        {
            this.deactivateOnIdle();
        }
    }

    public interface ISimpleRecoverableStreamProcessor<TState, TEvent>
    {
        Task<bool> OnEventAsync(TState state, TEvent @event);
    }

    public interface ISimpleRecoverableStream<TState, TEvent> where TState : new()
    {
        IStreamIdentity StreamId { get; }

        TState State { get; }

        void Attach(
            ISimpleRecoverableStreamProcessor<TState, TEvent> processor,
            IAdvancedStorage<RecoverableStreamState<TState>> storage,
            IRecoverableStreamStoragePolicy storagePolicy);

        Task OnActivateAsync();

        Task OnDeactivateAsync();
    }

    public sealed class RecoverableStreamLogger<TLogContext, TState> : IDisposable
        where TLogContext : IDisposable
    {
        private readonly IRecoverableStreamLogs<TLogContext, TState> logs;

        private readonly IStreamIdentity streamId;
        private readonly string methodName;
        private StreamSequenceToken lastToken;
        private StreamSequenceToken currentToken;
        private TState state;

        private TLogContext context;

        public RecoverableStreamLogger(
            IRecoverableStreamLogs<TLogContext, TState> logs,
            RecoverableStreamState<TState> streamState,
            string methodName,
            StreamSequenceToken currentToken)
        {
            if (logs == null) { throw new ArgumentNullException(nameof(logs)); }
            this.ValidateStreamState(streamState);
            if (methodName == null) { throw new ArgumentNullException(nameof(methodName)); }

            this.logs = logs;

            this.streamId = streamState?.StreamId;
            this.methodName = methodName;
            this.lastToken = streamState?.GetToken();
            this.currentToken = currentToken;

            if (streamState != null)
            {
                this.state = streamState.ApplicationState;
            }

            this.Refresh();
        }

        public IStreamIdentity StreamId => this.streamId;

        public string MethodName => this.methodName;

        public StreamSequenceToken LastToken
        {
            get => this.lastToken;
            set
            {
                this.lastToken = value;
                this.Refresh();
            }
        }

        public StreamSequenceToken CurrentToken
        {
            get => this.currentToken;
            set
            {
                this.currentToken = value;
                this.Refresh();
            }
        }

        public TState State
        {
            get => this.state;
            set
            {
                this.state = value;
                this.Refresh();
            }
        }

        public void Dispose()
        {
            this.Close();
        }

        private void Open()
        {
            if (this.context != null)
            {
                throw new InvalidOperationException("Tried to open a context when one was already open. This shouldn't happen.");
            }

            try
            {
                this.context = this.logs.CreateContext(this.StreamId, this.MethodName, this.LastToken, this.CurrentToken, this.State);
            }
            catch (Exception exception)
            {
                this.logs.ContextCreateFailedWithException(exception);
                throw;
            }
        }

        private void Close()
        {
            if (this.context == null)
            {
                return;
            }

            try
            {
                this.context.Dispose();
            }
            catch (Exception exception)
            {
                this.logs.ContextDisposeFailedWithException(exception);
                throw;
            }

            this.context = default;
        }

        private void Refresh()
        {
            this.Close();
            this.Open();
        }

        private void ValidateStreamState(RecoverableStreamState<TState> streamState)
        {
            if (streamState == null)
            {
                return;
            }

            if (streamState.StreamId == null)
            {
                throw new ArgumentException("Stream ID should be set", nameof(streamState));
            }
        }

        public void UpdateStreamState(RecoverableStreamState<TState> streamState)
        {
            if (streamState == null) { throw new ArgumentNullException(nameof(streamState), "Stream State shouldn't become null"); }
            this.ValidateStreamState(streamState);

            if (this.streamId.Guid != streamState.StreamId.Guid ||
                this.streamId.Namespace != streamState.StreamId.Namespace)
            {
                throw new InvalidOperationException("Stream ID shouldn't change");
            }

            this.lastToken = streamState.GetToken();
            this.State = streamState.ApplicationState;

            this.Refresh();
        }

        public void Log(Func<IRecoverableStreamLogs<TLogContext, TState>, Action<TLogContext>> log) => log.Invoke(this.logs).Invoke(this.context);
        public void Log<T1>(Func<IRecoverableStreamLogs<TLogContext, TState>, Action<TLogContext, T1>> log, T1 arg1) => log.Invoke(this.logs).Invoke(this.context, arg1);
        public void Log<T1, T2>(Func<IRecoverableStreamLogs<TLogContext, TState>, Action<TLogContext, T1, T2>> log, T1 arg1, T2 arg2) => log.Invoke(this.logs).Invoke(this.context, arg1, arg2);
        public void Log<T1, T2, T3>(Func<IRecoverableStreamLogs<TLogContext, TState>, Action<TLogContext, T1, T2, T3>> log, T1 arg1, T2 arg2, T3 arg3) => log.Invoke(this.logs).Invoke(this.context, arg1, arg2, arg3);
        public void Log<T1, T2, T3, T4>(Func<IRecoverableStreamLogs<TLogContext, TState>, Action<TLogContext, T1, T2, T3, T4>> log, T1 arg1, T2 arg2, T3 arg3, T4 arg4) => log.Invoke(this.logs).Invoke(this.context, arg1, arg2, arg3, arg4);
        public void Log<T1, T2, T3, T4, T5>(Func<IRecoverableStreamLogs<TLogContext, TState>, Action<TLogContext, T1, T2, T3, T4, T5>> log, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5) => log.Invoke(this.logs).Invoke(this.context, arg1, arg2, arg3, arg4, arg5);
    }

    public interface IRecoverableStreamLogs<TLogContext, TState>
        where TLogContext : IDisposable
    {
        TLogContext CreateContext(IStreamIdentity streamId, string methodName, StreamSequenceToken lastSequenceToken, StreamSequenceToken currentSequenceToken, TState state);
        
        void ContextCreateFailedWithException(IStreamIdentity streamId, string methodName, StreamSequenceToken lastSequenceToken, StreamSequenceToken currentSequenceToken, TState state, Exception exception);

        void ContextDisposeFailedWithException(IStreamIdentity streamId, string methodName, StreamSequenceToken lastSequenceToken, StreamSequenceToken currentSequenceToken, TState state, Exception exception);
        
        void ActivateInvoked(TLogContext context);
        void ActivateCreatingNewState(TLogContext context);
        void ActivateFoundExistingState(TLogContext context);
        void ActivateFailedWithException(TLogContext context, Exception exception);
        void ActivateSucceeded(TLogContext context);
    }

    // TODO List:
    //   - Logging
    //   - Context scopes (Activity ID, AutoEvents context, etc.)
    public class SimpleRecoverableStream<TState, TEvent, TLogContext> : ISimpleRecoverableStream<TState, TEvent>
        where TState : new()
        where TLogContext : IDisposable
    {
        private readonly IOrleansHooks<TEvent> orleansHooks;

        private ISimpleRecoverableStreamProcessor<TState, TEvent> processor;
        private RecoverableStreamStorage<TState> storage;
        private IRecoverableStreamLogs<TLogContext, TState> logs;

        public SimpleRecoverableStream(IStreamIdentity streamId, IOrleansHooks<TEvent> orleansHooks)
        {
            if (streamId == null) { throw new ArgumentNullException(nameof(streamId)); }
            if (orleansHooks == null) { throw new ArgumentNullException(nameof(orleansHooks)); }

            this.StreamId = streamId;
            this.orleansHooks = orleansHooks;
        }

        public IStreamIdentity StreamId { get; }

        public TState State
        {
            get
            {
                if (this.storage?.State == null)
                {
                    return default;
                }

                return this.storage.State.ApplicationState;
            }
        }

        public void Attach(
            ISimpleRecoverableStreamProcessor<TState, TEvent> processor,
            IAdvancedStorage<RecoverableStreamState<TState>> storage,
            IRecoverableStreamStoragePolicy storagePolicy,
            IRecoverableStreamLogs<TLogContext, TState> logs)
        {
            if (processor == null) { throw new ArgumentNullException(nameof(processor)); }
            if (storage == null) { throw new ArgumentNullException(nameof(storage)); }
            if (storagePolicy == null) { throw new ArgumentNullException(nameof(storagePolicy)); }
            if (logs == null) { throw new ArgumentNullException(nameof(logs)); }

            if (this.processor != null)
            {
                throw new InvalidOperationException("Stream is already attached.");
            }

            this.processor = processor;
            this.storage = new RecoverableStreamStorage<TState>(storage, storagePolicy);
            this.logs = logs;
        }

        public async Task OnActivateAsync()
        {
            this.CheckAttached();

            using (var logger = this.CreateLogger())
            {
                try
                {
                    logger.Log(l => l.ActivateInvoked);

                    await this.storage.Load();
                    if (this.storage.State == null) // TODO: Will this actually come back null? What's the expectation from Orleans IStorage?
                    {
                        this.storage.State = new RecoverableStreamState<TState>
                        {
                            StreamId = this.StreamId,
                            ApplicationState = new TState()
                        };

                        logger.UpdateStreamState(this.storage.State);
                        logger.Log(l => l.ActivateCreatingNewState);
                    }
                    else
                    {
                        logger.UpdateStreamState(this.storage.State);
                        logger.Log(l => l.ActivateFoundExistingState);
                    }

                    await this.SubscribeAsync();

                    logger.Log(l => l.ActivateSucceeded);
                }
                catch (Exception exception)
                {
                    logger.Log(l => l.ActivateFailedWithException, exception);
                    throw;
                }
            }
        }

        public Task OnDeactivateAsync()
        {
            this.CheckAttached();

            // TODO: Add an optimization where this doesn't save unless the token is different
            return this.storage.Save();
        }

        public async Task OnEventAsync(TEvent @event, StreamSequenceToken token)
        {
            this.CheckAttached();

            try
            {
                if (this.storage.State.IsDuplicateEvent(token))
                {
                    return;
                }

                // Save the start token so that if we enter recovery the token isn't null
                if (this.storage.State.StartToken == null)
                {
                    this.storage.State.SetStartToken(token);

                    var saveFastForwarded = await this.SaveAsync();

                    if (saveFastForwarded)
                    {
                        // We fast-forwarded so it's possible that we skipped over this event and it is now considered a duplicate. Reevaluate its duplicate status.
                        if (this.storage.State.IsDuplicateEvent(token))
                        {
                            return;
                        }
                    }
                }

                this.storage.State.SetCurrentToken(token);

                var processorRequestsSave = await this.processor.OnEventAsync(this.storage.State.ApplicationState, @event);

                if (processorRequestsSave)
                {
                    await this.SaveAsync();
                }
                else
                {
                    await this.CheckpointIfOverdueAsync();
                }
            }
            catch
            {
                this.orleansHooks.DeactivateOnIdle();

                throw;
            }
        }

        public Task OnErrorAsync(Exception exception)
        {
            return Task.CompletedTask;
        }

        private void CheckAttached()
        {
            if (this.processor == null)
            {
                throw new InvalidOperationException($"Stream has not been attached. Call {nameof(this.Attach)} before calling other methods.");
            }
        }

        private RecoverableStreamLogger<TLogContext, TState> CreateLogger(
            StreamSequenceToken currentSequenceToken = null,
            [CallerMemberName]string callerMethodName = null)
        {
            return new RecoverableStreamLogger<TLogContext, TState>(this.logs, this.storage.State, callerMethodName, currentSequenceToken);
        }

        private Task SubscribeAsync()
        {
            return this.orleansHooks.SubscribeAsync(this.StreamId, this.OnEventAsync, this.OnErrorAsync, this.storage.State.GetToken());
        }

        private Task<bool> CheckpointIfOverdueAsync()
        {
            return this.PersistAsync(isCheckpoint: true);
        }

        private Task<bool> SaveAsync()
        {
            return this.PersistAsync(isCheckpoint: false);
        }

        private async Task<bool> PersistAsync(bool isCheckpoint)
        {
            bool fastForwardRequested;
            if (isCheckpoint)
            {
                // TODO: We don't currently need checkpoint to return if it actually needed to save
                (_, fastForwardRequested) = await this.storage.CheckpointIfOverdue();
            }
            else
            {
                fastForwardRequested = await this.storage.Save();
            }

            if (fastForwardRequested)
            {
                await this.SubscribeAsync();
            }

            return fastForwardRequested;
        }
    }

    public class SimpleState
    {
    }
    
    public class SimpleEvent
    {
    }

    public class HaloStreamProviderNameResolver : IStreamProviderNameResolver
    {
        public string GetStreamProviderName(string streamNamespace) => throw new NotImplementedException();
    }

    public class SimpleRecoverableStreamProcessorBase<TState, TEvent> : ISimpleRecoverableStreamProcessor<TState, TEvent>
    {
        public virtual Task OnActivateAsync(TState readonlyState, StreamSequenceToken token)
        {
            return Task.CompletedTask;
        }

        public virtual Task<bool> OnEventAsync(TState state, TEvent @event)
        {
            return Task.FromResult(false);
        }

        public virtual Task OnDeactivateAsync(TState readonlyState, StreamSequenceToken token)
        {
            return Task.CompletedTask;
        }

        public virtual Task OnFastForwardAsync(TState readonlyState, StreamSequenceToken token)
        {
            return Task.CompletedTask;
        }

        public virtual Task OnRecoveryAsync(TState readonlyState, StreamSequenceToken token, TEvent @event)
        {
            return Task.CompletedTask;
        }

        public virtual Task OnErrorAsync(TState readonlyState, StreamSequenceToken token, Exception exception)
        {
            return Task.CompletedTask;
        }
    }

    public class HaloStreamProcessor : SimpleRecoverableStreamProcessorBase<SimpleState, SimpleEvent>
    {
        public override Task<bool> OnEventAsync(SimpleState state, SimpleEvent @event)
        {
            // TODO: Setup if dynamic stats isn't setup or state is different, setup dynamic stats
            // TODO: Ingest event into dynamic stats
            throw new NotImplementedException();
        }
    }

    public class HaloAdvancedStorage<TState> : IAdvancedStorage<TState>
    {
        public TState State { get; set; }

        public string ETag { get; }

        public Task<AdvancedStorageReadResultCode> ReadStateAsync() => throw new NotImplementedException();

        public Task<AdvancedStorageWriteResultCode> WriteStateAsync() => throw new NotImplementedException();
    }

    public class HaloStoragePolicy : IRecoverableStreamStoragePolicy
    {
        public TimeSpan CheckpointTimerPeriod { get; }

        public TimeSpan GetNextCheckpoint(int checkpointAttemptCount) => throw new NotImplementedException();

        public int GetCheckpointSubAttemptCount(int checkpointAttemptCount) => throw new NotImplementedException();

        public TimeSpan GetReadBackoff(AdvancedStorageReadResultCode resultCode, int attempts) => throw new NotImplementedException();

        public bool ShouldBackoffOnWriteWithAmbiguousResult { get; }

        public bool ShouldReloadOnWriteWithAmbiguousResult { get; }

        public TimeSpan GetWriteBackoff(AdvancedStorageWriteResultCode resultCode, int attempts) => throw new NotImplementedException();
    }

    public class SimpleRecoverableStreamingGrain : Grain
    {
        private readonly ISimpleRecoverableStream<SimpleState, SimpleEvent> stream;

        // Orleans ctor, invoked via DI
        public SimpleRecoverableStreamingGrain(IGrainActivationContext grainActivationContext)
        {
            if (grainActivationContext == null) { throw new ArgumentNullException(nameof(grainActivationContext)); }

            // TODO: I'm not sure if this is set by default or if we need to opt-in and configure Orleans shenanigans
            Guid streamGuid = grainActivationContext.GrainIdentity.GetPrimaryKey(out string streamNamespace);
            var streamId = new StreamIdentity(streamGuid, streamNamespace);

            var orleansHooks = new OrleansHooks<SimpleEvent>(
                new HaloStreamProviderNameResolver(),
                this.GetStreamProvider,
                this.DeactivateOnIdle);

            this.stream = new SimpleRecoverableStream<SimpleState, SimpleEvent>(streamId, orleansHooks);

            var streamProcessor = new HaloStreamProcessor();

            var storage = new HaloAdvancedStorage<RecoverableStreamState<SimpleState>>();

            var storagePolicy = new HaloStoragePolicy();

            this.stream.Attach(streamProcessor, storage, storagePolicy);
        }

        public override async Task OnActivateAsync()
        {
            await this.stream.OnActivateAsync();

            await base.OnActivateAsync();
        }

        public override async Task OnDeactivateAsync()
        {
            await this.stream.OnDeactivateAsync();

            await base.OnDeactivateAsync();
        }
    }
}