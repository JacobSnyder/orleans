using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
            IRecoverableStreamStoragePolicy storagePolicy,
            IRecoverableStreamLogEvents logEvents,
            IRecoverableStreamLogScopeFactory<TState> logScopeFactory);

        Task OnActivateAsync();

        Task OnDeactivateAsync();
    }
    public sealed class DefaultRecoverableStreamLoggerScope : IDisposable
    {
        public static IDisposable Instance { get; } = new DefaultRecoverableStreamLoggerScope();

        private DefaultRecoverableStreamLoggerScope()
        {
        }

        public void Dispose()
        {
        }
    }

    // TODO: There should be a "master" logger that is owned by everybody. That guy should own the current scope. Everybody else can be constructed with him. That way we don't have to pass him around all over. If somebody tries to log and we don't have a scope we should log. Scopes can be opened. Unclear if we should allow anybody to update the current scope or only the person that opened it. For now, let's assume that it's only the guy that opened the scope. It's a slippery slope because if we allow anybody to update the current scope, should we allow anybody to close it? Thinking more about it I think we should allow anybody to update the current scope so that we can have the storage layer update it if it needed it load.
    public sealed class RecoverableStreamLogger<TState> : IDisposable
    {
        private readonly IRecoverableStreamLogScopeFactory<TState> logScopeFactory;
        private readonly IStreamIdentity streamId;

        private IDisposable currentScope;

        public RecoverableStreamLogger(
            IRecoverableStreamLogEvents logEvents,
            IRecoverableStreamLogScopeFactory<TState> logScopeFactory,
            IStreamIdentity streamId)
        {
            if (logEvents == null) { throw new ArgumentNullException(nameof(logEvents)); }
            if (logScopeFactory == null) { throw new ArgumentNullException(nameof(logScopeFactory)); }
            if (streamId == null) { throw new ArgumentNullException(nameof(streamId)); }

            this.LogEvents = logEvents;
            this.logScopeFactory = logScopeFactory;
            this.streamId = streamId;
        }

        public IRecoverableStreamLogEvents LogEvents { get; }

        public void Dispose()
        {
            this.CloseScope(null, null, null);
        }

        public IDisposable OpenScope(RecoverableStreamState<TState> streamingState, StreamSequenceToken currentToken, [CallerMemberName] string methodName = null)
        {
            if (this.currentScope != null)
            {
                this.LogEvents.LoggerOpenScopeInvokedWhenScopeAlreadyOpen();
                return this.currentScope;
            }

            TState applicationState = default;
            if (streamingState != null)
            {
                applicationState = streamingState.ApplicationState;
            }

            try
            {
                this.currentScope = this.logScopeFactory.BeginScope(this.streamId, methodName, streamingState?.GetToken(), currentToken, applicationState);

                if (this.currentScope == null)
                {
                    this.currentScope = DefaultRecoverableStreamLoggerScope.Instance;
                }
            }
            catch (Exception exception)
            {
                this.LogEvents.LoggerOpenScopeFailedToBeginScopeWithException(this.streamId, methodName, streamingState?.GetToken(), currentToken, exception);
            }

            return this.currentScope;
        }

        public void UpdateScope(RecoverableStreamState<TState> streamingState, StreamSequenceToken currentToken, [CallerMemberName] string methodName = null)
        {
            if (this.currentScope == null)
            {
                this.LogEvents.LoggerUpdateScopeInvokedWhenNoScopeOpen(this.streamId, methodName, streamingState?.GetToken(), currentToken);
                return;
            }

            this.CloseScope(streamingState, currentToken, methodName);
            this.OpenScope(streamingState, currentToken, methodName);
        }

        private void CloseScope(RecoverableStreamState<TState> streamingState, StreamSequenceToken currentToken, string methodName)
        {
            if (this.currentScope == null)
            {
                return;
            }

            try
            {
                this.currentScope.Dispose();
            }
            catch (Exception exception)
            {
                this.LogEvents.LoggerCloseScopeFailedToDisposeWithException(this.streamId, methodName, streamingState?.GetToken(), currentToken, exception);
            }
            finally
            {
                this.currentScope = null;
            }
        }
    }

    public interface IRecoverableStreamLogScopeFactory<TState>
    {
        IDisposable BeginScope(IStreamIdentity streamId, string methodName, StreamSequenceToken lastToken, StreamSequenceToken currentToken, TState state);
    }

    public class DefaultRecoverableStreamLogScopeFactory<TState> : IRecoverableStreamLogScopeFactory<TState>
    {
        private readonly ILogger logger;
        private readonly IRecoverableStreamLogScopeApplicationScopeStateProducer<TState> applicationScopeStateProducer;

        public DefaultRecoverableStreamLogScopeFactory(ILogger logger, IRecoverableStreamLogScopeApplicationScopeStateProducer<TState> applicationScopeStateProducer)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }
            if (applicationScopeStateProducer == null) { throw new ArgumentNullException(nameof(applicationScopeStateProducer)); }

            this.logger = logger;
            this.applicationScopeStateProducer = applicationScopeStateProducer;
        }

        public IDisposable BeginScope(IStreamIdentity streamId, string methodName, StreamSequenceToken lastToken, StreamSequenceToken currentToken, TState state)
        {
            var scopeState = new List<KeyValuePair<string, object>>
            {
                new KeyValuePair<string, object>("StreamNamespace", streamId?.Namespace),
                new KeyValuePair<string, object>("StreamGuid", streamId?.Guid),
                new KeyValuePair<string, object>("LastToken", lastToken),
                new KeyValuePair<string, object>("CurrentToken", currentToken)
            };

            var applicationScopeState = this.applicationScopeStateProducer.GetState(state);
            if (applicationScopeState != null)
            {
                scopeState.AddRange(applicationScopeState);
            }

            return this.logger.BeginScope(scopeState);
        }
    }

    public interface IRecoverableStreamLogScopeApplicationScopeStateProducer<TState>
    {
        IEnumerable<KeyValuePair<string, object>> GetState(TState state);
    }

    public class DefaultRecoverableStreamLogScopeApplicationScopeStateProducer<TState> : IRecoverableStreamLogScopeApplicationScopeStateProducer<TState>
    {
        public IEnumerable<KeyValuePair<string, object>> GetState(TState state) => Array.Empty<KeyValuePair<string, object>>();
    }

    public class HaloRecoverableStreamLogScopeApplicationScopeStateProducer : IRecoverableStreamLogScopeApplicationScopeStateProducer<HaloState>
    {
        public IEnumerable<KeyValuePair<string, object>> GetState(HaloState state)
        {
            return new[]
            {
                new KeyValuePair<string, object>(nameof(HaloState.MatchId), state.MatchId), 
            };
        }
    }

    public class HaloRecoverableStreamLogScopeFactory : IRecoverableStreamLogScopeFactory<HaloState>
    {
        public IDisposable BeginScope(IStreamIdentity streamId, string methodName, StreamSequenceToken lastToken, StreamSequenceToken currentToken, HaloState state)
        {
            // TODO: Set ActivityId scope
            // TODO: Set AutoEvents context
            return null;
        }
    }

    public interface IRecoverableStreamLogEvents
    {
        void LoggerOpenScopeInvokedWhenScopeAlreadyOpen();
        void LoggerOpenScopeFailedToBeginScopeWithException(IStreamIdentity streamId, string methodName, StreamSequenceToken lastSequenceToken, StreamSequenceToken currentSequenceToken, Exception exception);
        void LoggerUpdateScopeInvokedWhenNoScopeOpen(IStreamIdentity streamId, string methodName, StreamSequenceToken lastToken, StreamSequenceToken currentToken);
        void LoggerCloseScopeFailedToDisposeWithException(IStreamIdentity streamId, string methodName, StreamSequenceToken lastSequenceToken, StreamSequenceToken currentSequenceToken, Exception exception);

        void ActivateInvoked();
        void ActivateCreatingNewState();
        void ActivateFoundExistingState();
        void ActivateFailedWithException(Exception exception);
        void ActivateSucceeded();
        void DeactivateInvoked();
        void DeactivateSucceeded();
        void DeactivateFailedWithException(Exception exception);
        void OnEventInvoked();
        void OnEventEncounteredDuplicateEvent();
        void OnEventSavingStartToken();
        void OnEventSavingAsRequestedByProcessor();
        void OnEventSucceeded();
        void OnEventFailedWithException(Exception exception);
        void OnErrorInvokedWithException(Exception exception);
        void SubscribeSucceeded(StreamSequenceToken token);
        void SubscribeFailedWithException(StreamSequenceToken token, Exception exception);
    }

    public class DefaultRecoverableStreamLogEvents : IRecoverableStreamLogEvents
    {
        private readonly ILogger logger;

        public DefaultRecoverableStreamLogEvents(ILogger logger)
        {
            if (this.logger == null) { throw new ArgumentNullException(nameof(logger)); }

            this.logger = logger;
        }

        public void LoggerOpenScopeInvokedWhenScopeAlreadyOpen() => this.logger.LogWarning("Logger OpenScope() invoked when there was already an open scope. The invocation will be ignored and the previously open scope will continue to be used.");

        public void LoggerOpenScopeFailedToBeginScopeWithException(IStreamIdentity streamId, string methodName, StreamSequenceToken lastSequenceToken, StreamSequenceToken currentSequenceToken, Exception exception) => throw new NotImplementedException();

        public void LoggerUpdateScopeInvokedWhenNoScopeOpen(IStreamIdentity streamId, string methodName, StreamSequenceToken lastToken, StreamSequenceToken currentToken) => throw new NotImplementedException();

        public void LoggerCloseScopeFailedToDisposeWithException(IStreamIdentity streamId, string methodName, StreamSequenceToken lastSequenceToken, StreamSequenceToken currentSequenceToken, Exception exception) => throw new NotImplementedException();

        public void ActivateInvoked() => throw new NotImplementedException();

        public void ActivateCreatingNewState() => throw new NotImplementedException();

        public void ActivateFoundExistingState() => throw new NotImplementedException();

        public void ActivateFailedWithException(Exception exception) => throw new NotImplementedException();

        public void ActivateSucceeded() => throw new NotImplementedException();
    }

    public class HaloRecoverableStreamLogEvents : IRecoverableStreamLogEvents
    {
        public void LoggerOpenScopeInvokedWhenScopeAlreadyOpen() => throw new NotImplementedException();

        public void LoggerOpenScopeFailedToBeginScopeWithException(IStreamIdentity streamId, string methodName, StreamSequenceToken lastSequenceToken, StreamSequenceToken currentSequenceToken, Exception exception) => throw new NotImplementedException();

        public void LoggerUpdateScopeInvokedWhenNoScopeOpen(IStreamIdentity streamId, string methodName, StreamSequenceToken lastToken, StreamSequenceToken currentToken) => throw new NotImplementedException();

        public void LoggerCloseScopeFailedToDisposeWithException(IStreamIdentity streamId, string methodName, StreamSequenceToken lastSequenceToken, StreamSequenceToken currentSequenceToken, Exception exception) => throw new NotImplementedException();

        public void ActivateInvoked() => throw new NotImplementedException();

        public void ActivateCreatingNewState() => throw new NotImplementedException();

        public void ActivateFoundExistingState() => throw new NotImplementedException();

        public void ActivateFailedWithException(Exception exception) => throw new NotImplementedException();

        public void ActivateSucceeded() => throw new NotImplementedException();

        public void DeactivateInvoked() => throw new NotImplementedException();

        public void DeactivateSucceeded() => throw new NotImplementedException();

        public void DeactivateFailedWithException(Exception exception) => throw new NotImplementedException();

        public void OnEventInvoked() => throw new NotImplementedException();

        public void OnEventEncounteredDuplicateEvent() => throw new NotImplementedException();

        public void OnEventSavingStartToken() => throw new NotImplementedException();

        public void OnEventSavingAsRequestedByProcessor() => throw new NotImplementedException();

        public void OnEventSucceeded() => throw new NotImplementedException();

        public void OnEventFailedWithException(Exception exception) => throw new NotImplementedException();

        public void OnErrorInvokedWithException(Exception exception) => throw new NotImplementedException();

        public void SubscribeSucceeded(StreamSequenceToken token) => throw new NotImplementedException();

        public void SubscribeFailedWithException(StreamSequenceToken token, Exception exception) => throw new NotImplementedException();
    }

    public class SimpleRecoverableStream<TState, TEvent> : ISimpleRecoverableStream<TState, TEvent>
        where TState : new()
    {
        private readonly IOrleansHooks<TEvent> orleansHooks;

        private ISimpleRecoverableStreamProcessor<TState, TEvent> processor;
        private RecoverableStreamLogger<TState> logger;
        private RecoverableStreamStorage<TState> storage;

        private StreamSequenceToken expectedNextSequenceToken;

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
            IRecoverableStreamLogEvents logEvents,
            IRecoverableStreamLogScopeFactory<TState> logScopeFactory)
        {
            if (processor == null) { throw new ArgumentNullException(nameof(processor)); }
            if (storage == null) { throw new ArgumentNullException(nameof(storage)); }
            if (storagePolicy == null) { throw new ArgumentNullException(nameof(storagePolicy)); }
            if (logEvents == null) { throw new ArgumentNullException(nameof(logEvents)); }
            if (logScopeFactory == null) { throw new ArgumentNullException(nameof(logScopeFactory)); }

            if (this.processor != null)
            {
                throw new InvalidOperationException("Stream is already attached.");
            }

            this.processor = processor;
            this.logger = new RecoverableStreamLogger<TState>(logEvents, logScopeFactory, this.StreamId);
            this.storage = new RecoverableStreamStorage<TState>(storage, storagePolicy);
        }

        public async Task OnActivateAsync()
        {
            this.CheckAttached();

            using (this.logger.OpenScope(this.storage.State, currentToken: null))
            {
                try
                {
                    this.logger.LogEvents.ActivateInvoked();

                    await this.storage.Load();
                    if (this.storage.State == null) // TODO: Will this actually come back null? What's the expectation from Orleans IStorage?
                    {
                        this.storage.State = new RecoverableStreamState<TState>
                        {
                            StreamId = this.StreamId,
                            ApplicationState = new TState()
                        };

                        this.logger.UpdateScope(this.storage.State, currentToken: null);
                        this.logger.LogEvents.ActivateCreatingNewState();
                    }
                    else
                    {
                        this.logger.UpdateScope(this.storage.State, currentToken: null);
                        this.logger.LogEvents.ActivateFoundExistingState();
                    }

                    await this.SubscribeAsync();

                    this.logger.LogEvents.ActivateSucceeded();
                }
                catch (Exception exception)
                {
                    this.logger.LogEvents.ActivateFailedWithException(exception);
                    throw;
                }
            }
        }

        public async Task OnDeactivateAsync()
        {
            this.CheckAttached();

            using (this.logger.OpenScope(this.storage.State, currentToken: null))
            {
                try
                {
                    this.logger.LogEvents.DeactivateInvoked();

                    // TODO: Add an optimization where this doesn't save unless the token is different
                    await this.storage.Save();

                    this.logger.LogEvents.DeactivateSucceeded();
                }
                catch (Exception exception)
                {
                    this.logger.LogEvents.DeactivateFailedWithException(exception);
                    throw;
                }
            }
        }

        private async Task OnEventAsync(TEvent @event, StreamSequenceToken token)
        {
            using (this.logger.OpenScope(this.storage.State, token))
            {
                try
                {
                    this.logger.LogEvents.OnEventInvoked();

                    if (this.storage.State.IsDuplicateEvent(token))
                    {
                        this.logger.LogEvents.OnEventEncounteredDuplicateEvent();

                        return;
                    }

                    // Save the start token so that if we enter recovery the token isn't null
                    if (this.storage.State.StartToken == null)
                    {
                        this.logger.LogEvents.OnEventSavingStartToken();

                        this.storage.State.SetStartToken(token);

                        var saveFastForwarded = await this.SaveAsync();

                        if (saveFastForwarded)
                        {
                            // We fast-forwarded so it's possible that we skipped over this event and it is now considered a duplicate. Reevaluate its duplicate status.
                            if (this.storage.State.IsDuplicateEvent(token))
                            {
                                this.logger.LogEvents.OnEventEncounteredDuplicateEvent();

                                return;
                            }
                        }
                    }

                    this.storage.State.SetCurrentToken(token);

                    var processorRequestsSave = await this.processor.OnEventAsync(this.storage.State.ApplicationState, @event);

                    if (processorRequestsSave)
                    {
                        this.logger.LogEvents.OnEventSavingAsRequestedByProcessor();

                        await this.SaveAsync();
                    }
                    else
                    {
                        await this.CheckpointIfOverdueAsync();
                    }

                    this.logger.LogEvents.OnEventSucceeded();
                }
                catch (Exception exception)
                {
                    this.logger.LogEvents.OnEventFailedWithException(exception);

                    this.orleansHooks.DeactivateOnIdle();

                    throw;
                }
            }
        }

        private Task OnErrorAsync(Exception exception)
        {
            this.logger.LogEvents.OnErrorInvokedWithException(exception);

            return Task.CompletedTask;
        }

        private void CheckAttached()
        {
            if (this.processor == null)
            {
                throw new InvalidOperationException($"Stream has not been attached. Call {nameof(this.Attach)} before calling other methods.");
            }
        }

        private async Task SubscribeAsync()
        {
            var token = this.storage.State.GetToken();

            try
            {
                await this.orleansHooks.SubscribeAsync(this.StreamId, this.OnEventAsync, this.OnErrorAsync, token);

                this.expectedNextSequenceToken = token;

                this.logger.LogEvents.SubscribeSucceeded(token);
            }
            catch (Exception exception)
            {
                this.logger.LogEvents.SubscribeFailedWithException(token, exception);
                throw;
            }
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
            // TODO: Logging

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

    public class HaloState
    {
        public Guid MatchId { get; set; }
    }
    
    public class HaloEventBase
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

    public class HaloStreamProcessor : SimpleRecoverableStreamProcessorBase<HaloState, HaloEventBase>
    {
        public override Task<bool> OnEventAsync(HaloState state, HaloEventBase @event)
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

    [ImplicitStreamSubscription("TestStreamNamespace1")]
    [ImplicitStreamSubscription("TestStreamNamespace2")]
    public class SimpleRecoverableStreamingGrain : Grain
    {
        private readonly ISimpleRecoverableStream<HaloState, HaloEventBase> stream;

        // Orleans ctor, invoked via DI
        public SimpleRecoverableStreamingGrain(IGrainActivationContext grainActivationContext)
        {
            if (grainActivationContext == null) { throw new ArgumentNullException(nameof(grainActivationContext)); }

            // TODO: I'm not sure if this is set by default or if we need to opt-in and configure Orleans shenanigans
            Guid streamGuid = grainActivationContext.GrainIdentity.GetPrimaryKey(out string streamNamespace);
            var streamId = new StreamIdentity(streamGuid, streamNamespace);

            var orleansHooks = new OrleansHooks<HaloEventBase>(
                new HaloStreamProviderNameResolver(),
                this.GetStreamProvider,
                this.DeactivateOnIdle);

            this.stream = new SimpleRecoverableStream<HaloState, HaloEventBase>(streamId, orleansHooks);

            var streamProcessor = new HaloStreamProcessor();

            var storage = new HaloAdvancedStorage<RecoverableStreamState<HaloState>>();

            var storagePolicy = new HaloStoragePolicy();

            var logEvents = new HaloRecoverableStreamLogEvents();
            var logScopeFactory = new HaloRecoverableStreamLogScopeFactory();

            this.stream.Attach(streamProcessor, storage, storagePolicy, logEvents, logScopeFactory);
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