using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    public interface IOrleansHooks
    {
        Task SubscribeAsync<TEvent>(
            Func<TEvent, StreamSequenceToken, Task> onNextAsync,
            Func<Exception, Task> onErrorAsync,
            StreamSequenceToken token);

        void DeactivateOnIdle();
    }

    // For now, we assume that the only things that are allowed to modify state is the method that returns a bool, OnEvent(). Everything else is just for notification purposes only and allows implementations to initialize and cleanup processing (e.g. Aggregates or Dynamic Stats). Going forward we will consider expanding this. This reasons for this restriction are two-fold. First, we want to keep the scope of this initial implementation manageable. Secondly, there are deployment concerns. The current assumption is that the state will be equivalent given matching sequence tokens. If we start to modify state outside of OnEvent(), we will be modifying the state without changing the sequence token, thus violating this assumption. We will need a way to resolve that (potentially by implementing some kind of tie-breaking mechanism either by incrementing a "sub-token" or by persisting out the various "additional" actions that get invoked).
    public interface ISimpleRecoverableStreamProcessor<TState, TEvent>
    {
        Task OnActivateAsync(TState readonlyState, StreamSequenceToken token);

        Task<bool> OnEventAsync(TState state, StreamSequenceToken token, TEvent @event);

        Task OnDeactivateAsync(TState readonlyState, StreamSequenceToken token);

        Task OnFastForwardAsync(TState readonlyState, StreamSequenceToken token);

        Task OnErrorAsync(TState readonlyState, StreamSequenceToken token, Exception exception);
    }

    // TODO List:
    //   - Logging
    //   - Context scopes (Activity ID, AutoEvents context, etc.)
    //   - Recovery
    //   - Grain
    public class SimpleRecoverableStream<TState, TEvent>
        where TState : new()
    {
        private readonly IOrleansHooks orleansHooks;

        private ISimpleRecoverableStreamProcessor<TState, TEvent> processor;
        private RecoverableStreamStorage<TState> storage;

        public SimpleRecoverableStream(IStreamIdentity streamId, IOrleansHooks orleansHooks)
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
                if (this.storage.State == null)
                {
                    return default;
                }

                return this.storage.State.ApplicationState;
            }
        }

        public void Attach(
            ISimpleRecoverableStreamProcessor<TState, TEvent> processor,
            IAdvancedStorage<RecoverableStreamState<TState>> storage,
            IRecoverableStreamStoragePolicy storagePolicy)
        {
            if (processor == null) { throw new ArgumentNullException(nameof(processor)); }
            if (storage == null) { throw new ArgumentNullException(nameof(storage)); }
            if (storagePolicy == null) { throw new ArgumentNullException(nameof(storagePolicy)); }

            if (this.processor != null)
            {
                throw new InvalidOperationException("Stream already has Processor attached");
            }

            this.processor = processor;
            this.storage = new RecoverableStreamStorage<TState>(storage, storagePolicy);
        }

        public async Task OnActivateAsync()
        {
            this.CheckProcessorAttached();

            await this.storage.Load();
            if (this.storage.State == null) // TODO: Will this actually come back null? What's the expectation from Orleans IStorage?
            {
                this.storage.State = new RecoverableStreamState<TState>
                {
                    StreamId = this.StreamId,
                    ApplicationState = new TState()
                };
            }

            var token = this.storage.State.GetToken();

            await this.SubscribeAsync();

            await this.processor.OnActivateAsync(this.storage.State.ApplicationState, this.storage.State.GetToken());
        }

        public async Task OnDeactivateAsync()
        {
            this.CheckProcessorAttached();

            await this.processor.OnDeactivateAsync(this.storage.State.ApplicationState, this.storage.State.GetToken());

            // TODO: Add an optimization where this doesn't save unless the token is different
            await this.storage.Save();
        }

        public async Task OnEventAsync(TEvent @event, StreamSequenceToken token)
        {
            this.CheckProcessorAttached();

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

            var processorRequestsSave =
                await this.processor.OnEventAsync(this.storage.State.ApplicationState, token, @event);

            if (processorRequestsSave)
            {
                await this.SaveAsync();
            }
            else
            {
                await this.CheckpointIfOverdueAsync();
            }
        }

        public Task OnErrorAsync(Exception exception)
        {
            this.CheckProcessorAttached();

            return this.processor.OnErrorAsync(this.storage.State.ApplicationState, this.storage.State.GetToken(), exception);
        }

        private void CheckProcessorAttached()
        {
            if (this.processor == null)
            {
                throw new InvalidOperationException("Stream does not have Processor attached");
            }
        }

        private Task SubscribeAsync()
        {
            return this.orleansHooks.SubscribeAsync<TEvent>(this.OnEventAsync, this.OnErrorAsync, this.storage.State.GetToken());
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

                await this.processor.OnFastForwardAsync(this.storage.State.ApplicationState,
                    this.storage.State.GetToken());
            }

            return fastForwardRequested;
        }
    }
}